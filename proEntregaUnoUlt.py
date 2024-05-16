import requests
import psycopg2
from datetime import datetime

BASE_URL = 'https://restcountries.com/v3.1/'
query = 'all'  # Consulta para obtener información de todos los países

redshiftConfig = {
    'dbname': 'data-engineer-database',
    'user': 'pabloemanuelcampestrini_coderhouse',
    'password': '9mF949o19e',
    'host': 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    'port': '5439'
}

def getCountries():
    url = f"{BASE_URL}{query}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error al obtener información de los países: {response.status_code}")
        return None


def createTable():
    conn = psycopg2.connect(**redshiftConfig)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS paisesTuristicos (
            
            nombre VARCHAR(255),
            poblacion INT,
            cantidad_lugares_turisticos INT,
            km_cuadrados FLOAT,
            fecha_ingesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def loadData(data):
    conn = psycopg2.connect(**redshiftConfig)
    cur = conn.cursor()
    try:
        for country in data:
            nombre = country['name']['common']
            poblacion = country.get('population', None)
            # Simulamos la cantidad de lugares turísticos
            cantidad_lugares_turisticos = len(country.get('languages', []))
            km_cuadrados = country.get('area', None)
            print(f"Cargando país: {nombre}, Población: {poblacion}, Lugares turísticos: {cantidad_lugares_turisticos}, Área: {km_cuadrados} km²")
            cur.execute("""
                INSERT INTO paisesTuristicos (nombre, poblacion, cantidad_lugares_turisticos, km_cuadrados)
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
if __name__ == "__main__":
    # Obtener información de todos los países
    data = getCountries()
    if data:
        print(f"Se han encontrado {len(data)} países en total.")
        
        # Crear tabla en Redshift
        createTable()
        print("Tabla creada en Amazon Redshift.")
        
        # Cargar datos en la tabla de Redshift
        loadData(data)
        print("Datos cargados en la tabla de Redshift.")
    else:
        print("No se pudo obtener información de los países.")
