FROM python:3-alpine

COPY ./db.py /app/db.py
COPY ./requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

VOLUME /working

WORKDIR /working

ENTRYPOINT ["python", "/app/db.py"]
