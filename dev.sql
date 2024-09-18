create function pg_temp.schema_current_status() returns table (current_version char(14), version_table_exists bool) as $$
begin
  if (select true from pg_catalog.pg_class where relname = '_schema_versions' and relkind = 'r') then
    return query select max(current_version), true from _schema_versions;
  else
    return query select null::char(14), false;
  end if;
end;
$$ language plpgsql;

select * from pg_temp.schema_current_status()
