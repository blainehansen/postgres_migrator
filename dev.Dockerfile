FROM rust:latest as builder
WORKDIR /usr/working/

RUN apt update && yes | apt install python3 python3-pip

# RUN pip install migra~=3.0.0 psycopg2-binary~=2.9.3
RUN pip install pgadmin4~=6.12 psycopg2-binary~=2.9.3
