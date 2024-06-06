from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2

BASE_URL = 'https://restcountries.com/v3.1/'
query = 'all'

redshiftConfig = {
    'dbname': 'data-engineer-database',
    'user': 'pabloemanuelcampestrini_coderhouse',
    'password': '9mF949o19e',
    'host': 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    'port': '5439'
}

def etl():
    def getCountries():
        url = f"{BASE_URL}{query}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error al obtener información de los países: {response.status_code}")
            return None

    def createTable():
        try:
            conn = psycopg2.connect(**redshiftConfig)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS paisesTuristicos3 (
                    id INT PRIMARY key identity, 
                    nombre VARCHAR(255),
                    poblacion INT,
                    cantidad_lugares_turisticos INT,
                    km_cuadrados FLOAT,
                    fecha_ingesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            print("Tabla creada en Amazon Redshift.")
        except Exception as e:
            print(f"Error al crear la tabla en Redshift: {str(e)}")
        finally:
            conn.close()

    def loadData(data):
        try:
            conn = psycopg2.connect(**redshiftConfig)
            cur = conn.cursor()
            for country in data:
                nombre = country['name']['common']
                poblacion = country.get('population', None)
                cantidad_lugares_turisticos = len(country.get('languages', []))
                km_cuadrados = country.get('area', None)
                print(f"Cargando país: {nombre}, Población: {poblacion}, Lugares turísticos: {cantidad_lugares_turisticos}, Área: {km_cuadrados} km²")
                cur.execute("""
                    INSERT INTO paisesTuristicos3 (nombre, poblacion, cantidad_lugares_turisticos, km_cuadrados)
                    VALUES (%s, %s, %s, %s)
                """, (nombre, poblacion, cantidad_lugares_turisticos, km_cuadrados))
            conn.commit()
            print("Datos cargados en la tabla de Redshift.")
        except Exception as e:
            conn.rollback()
            print(f"Error al cargar datos en Redshift: {str(e)}")
        finally:
            conn.close()

    # Ejecutar el proceso ETL
    data = getCountries()
    if data:
        print(f"Se han encontrado {len(data)} países en total.")
        createTable()
        loadData(data)
    else:
        print("No se pudo obtener información de los países.")

# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_etl_countries',
    default_args=default_args,
    description='ETL DAG para obtener y cargar información de países en Redshift',
    schedule_interval=timedelta(days=1),
)

# Definir el PythonOperator para ejecutar la función ETL
etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=etl,
    dag=dag,
)

etl_task
