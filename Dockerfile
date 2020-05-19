FROM python:3.7-slim-buster

# We copy just the requirements.txt first to leverage Docker cache
COPY ./requirements.txt /app/requirements.txt

ENV ES_SOURCE_HOST  http://elastic2.your-corp.net
ENV ES_DEST_HOST http://elastic6.your-corp.net
#ENV DYNAMIC_MAPPING False
#ENV DOCTYPE _doc
ENV OPTIMIZE_FOR_BULK True

# These fields (comma separated) will be migrated as pure raw types (not analyzed strings).
# This variable is specified per index 
#ENV NOT_ANALYZED_FIELDS_my_index_1=clusterId,emailAddress

WORKDIR /app

RUN pip install -r requirements.txt

COPY ./app.py /app/app.py
COPY ./es_mapping_migration.py /app/es_mapping_migration.py

CMD ["flask", "run", "--host", "0.0.0.0"]
