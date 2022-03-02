FROM python:3

RUN pip3 install migra~=3.0.0 psycopg2-binary~=2.9.3

WORKDIR /working

# ENTRYPOINT ["./migrator_binary"]


# the real docker image will be a multi-stage build
