FROM rust:latest as builder
WORKDIR /usr/working/

RUN apt update && yes | apt install python3 python3-pip

RUN rm /usr/lib/python3.11/EXTERNALLY-MANAGED && pip install migra~=3.0.0 psycopg2-binary~=2.9.3
