# Usar una imagen base ligera de Python
FROM python:3.8-slim

# Establecer variables de entorno necesarias para Airflow
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_VERSION=2.1.0
ENV PYTHON_VERSION=3.8

# Instalar dependencias necesarias
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    libpq-dev \
    && pip install --no-cache-dir apache-airflow==$AIRFLOW_VERSION \
    psycopg2-binary \
    requests

# Crear directorios necesarios
RUN mkdir -p $AIRFLOW_HOME/dags

# Copiar el DAG al directorio de Airflow
COPY dags/ $AIRFLOW_HOME/dags/

# Exponer el puerto de Airflow
EXPOSE 8080

# Comando para iniciar el scheduler y webserver de Airflow
CMD ["sh", "-c", "airflow db init && airflow scheduler & airflow webserver"]
