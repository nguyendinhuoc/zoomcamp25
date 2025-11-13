FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY ingest_data.py ingest_data.py
COPY taxi_zone_lookup.csv taxi_zone_lookup.csv

ENTRYPOINT [ "python", "ingest_data.py" ]