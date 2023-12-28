FROM apache/airflow:2.7.3

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow"

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt