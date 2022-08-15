# Using the https://www.pgadmin.org/docs/pgadmin4/development/schema_diff.html without the GUI
# The main idea is to init the flask app(which needs and creates a sqlitedb), fill in the required state(some sqlite rows)
# and interact with the schema diff endpoints with a test http client.
import argparse
import json
import logging
import os
import random
import re
import sqlite3
import sys
import threading
import time
from contextlib import redirect_stderr
from typing import Dict, List, Optional, Tuple

import psycopg2
from flask.testing import FlaskClient
from psycopg2 import extensions as ext

from pgadmin4 import config
from pgadmin4.pgadmin import create_app
from pgadmin4.pgadmin.model import SCHEMA_VERSION


def build_arg_parser():
	"""Build CLI Parser"""
	parser = argparse.ArgumentParser(
		description="Diff two databases or two schemas", prog="pgadmin-schema-diff"
	)
	parser.add_argument(
		"source",
		help="a postgres connection string for the source database(e.g. postgres://postgres@localhost/source)",
	)
	parser.add_argument(
		"target",
		help="a postgres connection string for the target database(e.g. postgres://postgres@localhost/target)",
	)
	# parser.add_argument(
	# 	"--schema",
	# 	help="schema to diff in both source and target database",
	# 	dest="schema",
	# )
	# parser.add_argument(
	# 	"--source-schema", help="source database schema", dest="source_schema"
	# )
	# parser.add_argument(
	# 	"--target-schema", help="target database schema", dest="target_schema"
	# )

	group = parser.add_mutually_exclusive_group()
	group.add_argument(
		"--include-objects",
		help="comma delimited database objects to include on the diff(e.g. table,sequence,function)",
		dest="include_objects",
		type=lambda s: re.split(",", s),
	)
	group.add_argument(
		"--exclude-objects",
		help="comma delimited database objects to exclude on the diff(e.g. table,sequence,function)",
		dest="exclude_objects",
		type=lambda s: re.split(",", s),
	)
	group.add_argument(
		"--json-diff",
		help="get the full diff output in json(for debugging, internal use)",
		dest="json_diff",
		action="store_true",
	)
	return parser


def configure() -> None:
	"""Configures PGAdmin internals"""

	os.environ["PGADMIN_TESTING_MODE"] = "1"

	config.SERVER_MODE = False
	config.WTF_CSRF_ENABLED = False

	# Removing some unnecessary modules(attempt to speed up init)
	config.MODULE_BLACKLIST = [
		"pgadmin.about",
		"pgadmin.authenticate",
		"pgadmin.browser.register_browser_preferences",
		"pgadmin.browser.server_groups.servers.pgagent",
		"pgadmin.browser.server_groups.servers.pgagent.schedules",
		"pgadmin.browser.server_groups.servers.pgagent.steps",
		"pgadmin.browser.server_groups.servers.pgagent.tests",
		"pgadmin.browser.server_groups.servers.pgagent.utils",
		"pgadmin.browser.server_groups.servers.pgagent.schedules.test",
		"pgadmin.browser.server_groups.servers.pgagent.steps.tests",
		"pgadmin.dashboard",
		"pgadmin.help",
		"pgadmin.misc",
		"pgadmin.preferences",
		"pgadmin.settings",
		"pgadmin.feature_tests",
		"pgadmin.tools.backup",
		"pgadmin.tools.datagrid",
		"pgadmin.tools.debugger",
		"pgadmin.tools.erd",
		"pgadmin.tools.grant_wizard",
		"pgadmin.tools.import_export",
		"pgadmin.tools.maintenance",
		"pgadmin.tools.restore",
		"pgadmin.tools.search_objects",
		"pgadmin.tools.sqleditor",
		"pgadmin.tools.storage_manager",
		"pgadmin.tools.user_management",
		"pgadmin.tools.backup.tests",
		"pgadmin.tools.datagrid.tests",
		"pgadmin.tools.debugger.tests",
		"pgadmin.tools.debugger.utils",
		"pgadmin.tools.erd.tests",
		"pgadmin.tools.erd.utils",
		"pgadmin.tools.grant_wizard.tests",
		"pgadmin.tools.import_export.tests",
		"pgadmin.tools.maintenance.tests",
		"pgadmin.tools.restore.tests",
		"pgadmin.tools.schema_diff.tests",
		"pgadmin.tools.search_objects.tests",
		"pgadmin.tools.search_objects.utils",
		"pgadmin.tools.sqleditor.command",
		"pgadmin.tools.sqleditor.tests",
		"pgadmin.tools.sqleditor.utils",
		"pgadmin.utils",
	]

	# Change these to DEBUG to see more details including the SQL queries
	config.CONSOLE_LOG_LEVEL = logging.ERROR
	config.FILE_LOG_LEVEL = logging.ERROR

	config.SETTINGS_SCHEMA_VERSION = SCHEMA_VERSION

	return None


def create_server(connection_string: str, schema: str) -> Tuple[int, Dict]:
	"""Registers a postgres server with local sqlite db"""
	arg = ext.parse_dsn(connection_string)

	server = {
		"name": str(
			random.randint(10000, 65535)
		),  ## some random name for registering the server on the sqlite db
		"db": arg["dbname"],
		"username": arg["user"],
		"db_password": arg.get("password"),
		"role": "",
		"sslmode": "prefer",
		"comment": "",
		"port": 5432,
		"password": "",
		"connstring": connection_string,
		"schema": schema,
		**arg,
	}

	conn = sqlite3.connect(config.TEST_SQLITE_PATH)
	cur = conn.cursor()
	server_details = (
		1,
		1,
		server["name"],
		server["host"],
		server["port"],
		server["db"],
		server["username"],
		server["role"],
		server["sslmode"],
		server["comment"],
	)
	cur.execute(
		"INSERT INTO server (user_id, servergroup_id, name, host, "
		"port, maintenance_db, username, role, ssl_mode,"
		" comment) VALUES (?,?,?,?,?,?,?,?,?,?)",
		server_details,
	)
	server_id = cur.lastrowid
	conn.commit()
	conn.close()
	return server_id, server


def build_app_test_client() -> FlaskClient:
	"""Creates a test client for the pgadmin4 flask app"""
	app = create_app()
	# Needed for solving: ERROR  pgadmin: 'PgAdmin' object has no attribute 'PGADMIN_INT_KEY'
	app.PGADMIN_INT_KEY = ""

	return app.test_client()


def get_diff_progress_id(test_client: FlaskClient) -> int:
	# Get trans_id(transaction id), it's just a random id generated on flask that's used for showing the diff progress messages
	res = test_client.get("schema_diff/initialize")
	response_data = json.loads(res.data.decode("utf-8"))
	trans_id = response_data["data"]["schemaDiffTransId"]
	return trans_id


def init_server_connection(
	test_client: FlaskClient, server_id: int, password: str
) -> None:
	# connect to both source and target servers
	test_client.post(
		"schema_diff/server/connect/{}".format(server_id),
		data=json.dumps({"password": password}),
		content_type="html/json",
	)
	return None


def init_database_connection(
	test_client: FlaskClient, server_id: int, db_id: int
) -> None:
	test_client.post("schema_diff/database/connect/{0}/{1}".format(server_id, db_id))
	return None


def get_db_and_schema_id(
	connection_string: str, schema: Optional[str]
) -> Tuple[int, Optional[int]]:
	# Get the source and target db/schema oids for diffing

	conn = psycopg2.connect(connection_string)
	cur = conn.cursor()
	cur.execute("select oid from pg_database where datname = current_database()")
	db_id = cur.fetchone()[0]
	if schema is None:
		return db_id, None
	cur.execute("select %s::regnamespace::oid", (schema,))
	schema_id = cur.fetchone()[0]
	cur.close()
	conn.close()
	return db_id, schema_id


def get_diffing_url(
	trans_id: int,
	server_id_1: int,
	src_db_id: int,
	src_schema_id: Optional[int],
	server_id_2: int,
	tar_db_id: int,
	tar_schema_id: Optional[int],
) -> str:

	if src_schema_id is not None and tar_schema_id is not None:
		comp_url = "schema_diff/compare_schema/{0}/{1}/{2}/{3}/{4}/{5}/{6}".format(
			trans_id,
			server_id_1,
			src_db_id,
			src_schema_id,
			server_id_2,
			tar_db_id,
			tar_schema_id,
		)
	else:
		# blaine: we always use this version
		# it seems all the important stuff happens in:
		# - fetch_compare_schemas
		# - compare_schema_objects
		comp_url = "schema_diff/compare_database/{0}/{1}/{2}/{3}/{4}".format(
			trans_id, server_id_1, src_db_id, server_id_2, tar_db_id
		)
	return comp_url


def get_schema_diff(
	trans_id: int,
	server_id_1: int,
	src_db_id: int,
	src_schema_id: Optional[int],
	server_id_2: int,
	tar_db_id: int,
	tar_schema_id: Optional[int],
) -> Dict:

	# Obtain the endpoint for diffing dbs or schemas
	comp_url = get_diffing_url(
		trans_id,
		server_id_1,
		src_db_id,
		src_schema_id,
		server_id_2,
		tar_db_id,
		tar_schema_id,
	)

	# Compute the schema diff on a thread so we can poll its progress
	# trick for getting the thread result on a variable to be used on main
	result = {"res": b""}

	def get_schema_diff(test_client, comp_url, result):
		response = test_client.get(comp_url)
		result["res"] = response.data

	diff_thread = threading.Thread(
		target=get_schema_diff, args=(test_client, comp_url, result)
	)
	diff_thread.start()

	# poll the progress
	while diff_thread.is_alive():
		res = test_client.get(f"schema_diff/poll/{trans_id}")
		res_data = json.loads(res.data.decode("utf-8"))
		data = res_data["data"]
		print(
			"{}...{}%".format(data["compare_msg"], data["diff_percentage"]),
			file=sys.stderr,
		)
		time.sleep(2)

	diff_thread.join()

	# {
	#   'success': 1,
	#   'errormsg': '',
	#   'info': '',
	#   'result': None,
	#   'data': [
	#       { 'id': 1, 'type': 'extension', ... },
	#   ]
	# }
	response_data = json.loads(result["res"].decode("utf-8"))
	return response_data


def display_diff(
	response_data,
	include_objects: List[str],
	exclude_objects: List[str],
	as_json: bool = False,
) -> None:
	"""Prints the schema diff"""

	def include_db_entity(entity) -> bool:
		"""Filter entities in the diff to user requested types"""
		if include_objects is not None:
			return entity in include_objects
		elif exclude_objects is not None:
			return entity not in exclude_objects
		return True

	if as_json:
		diff_result = json.dumps(response_data["data"], indent=4)
	else:
		# Some db objects on the json diff output don't have a diff_ddl(they're 'Identical') so we skip them.
		diff_result = "\n".join(
			x.get("diff_ddl")
			for x in response_data["data"]
			if x.get("status") != "Identical" and include_db_entity(x.get("type"))
		)

	if response_data["success"] == 1:
		print("Done.", file=sys.stderr)
		print(diff_result)
	else:
		print("Error: {}".format(response_data["errormsg"]), file=sys.stderr)


if __name__ == "__main__":

	parser = build_arg_parser()

	if len(sys.argv) < 2:
		parser.print_help()
		sys.exit(1)

	args = parser.parse_args()

	# Side effects
	configure()

	## Starting process
	print("Starting schema diff...", file=sys.stderr)

	# create_app prints "NOTE: Configuring authentication for DESKTOP mode.", this pollutes our SQL diff output.
	# So here we disable stdout temporarily to avoid that
	with open(os.devnull, "w") as devnull:
		with redirect_stderr(devnull):
			## Starts the Flask app, this step takes a while
			test_client = build_app_test_client()

	trans_id = get_diff_progress_id(test_client)

	server_id_1, server_1 = create_server(
		connection_string=args.source,
		schema=args.source_schema or args.schema,
	)
	server_id_2, server_2 = create_server(
		connection_string=args.target,
		schema=args.target_schema or args.schema,
	)

	init_server_connection(test_client, server_id_1, server_1["db_password"])
	init_server_connection(test_client, server_id_2, server_2["db_password"])

	# Get the source and target db/schema oids for diffing
	src_db_id, src_schema_id = get_db_and_schema_id(
		args.source, args.source_schema or args.schema
	)
	tar_db_id, tar_schema_id = get_db_and_schema_id(
		args.target, args.target_schema or args.schema
	)

	init_database_connection(test_client, server_id_1, src_db_id)
	init_database_connection(test_client, server_id_2, tar_db_id)

	response_data: Dict = get_schema_diff(
		trans_id,
		server_id_1,
		src_db_id,
		src_schema_id,
		server_id_2,
		tar_db_id,
		tar_schema_id,
	)

	display_diff(
		response_data,
		args.include_objects,
		args.exclude_objects,
		as_json=args.json_diff,
	)
