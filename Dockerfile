FROM quay.io/astronomer/astro-runtime:7.0.0
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
ENV GOOGLE_APPLICATION_CREDENTIALS=airflow-week-377305-1ffddaf51853.json
VOLUME $GOOGLE_APPLICATION_CREDENTIALS=airflow-week-377305-1ffddaf51853.json:ro
