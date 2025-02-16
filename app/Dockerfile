FROM python:3.11-slim

WORKDIR /code
COPY .. /code

# RUN pip install poetry && poetry config virtualenvs.create false && poetry install

RUN pip install -r requirements.txt

CMD ["python3", "kafka_processing.py"]
