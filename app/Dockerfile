FROM python:3.11.9

WORKDIR /code
COPY .. /code

# RUN pip install poetry && poetry config virtualenvs.create false && poetry install

RUN pip install -r requirements.txt

CMD ["python", "kafka_processing.py"]
