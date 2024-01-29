FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2 requests

# go to this directory
WORKDIR /app

# copy from outside the container to inside with the new name
COPY ingest_data.py ingest_data.py

# CMD ["python", "app.py"]
ENTRYPOINT ["python", "ingest_data.py"]