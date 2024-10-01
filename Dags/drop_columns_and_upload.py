from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import mysql.connector
import pandas as pd
import logging
import os

# Configuraciones
DATABASE = 'spotify_data'
TABLE = 'Spotify_Grammy_Joined'
DOWNLOAD_PATH = '/mnt/c/Users/Admin/Downloads/spotify_grammy_joined.csv'  # Ruta donde se guardará el CSV en la carpeta Downloads

# Función para conectarse a la base de datos MySQL
def conectar_mysql():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='password',  # Cambia esto a tu contraseña
        database=DATABASE
    )

# Función para eliminar columnas y exportar el CSV
def eliminar_columnas_y_exportar():
    logging.info("Conectando a MySQL...")
    connection = conectar_mysql()
    
    query = f"SELECT * FROM {TABLE}"
    df = pd.read_sql(query, connection)
    
    # Eliminar las columnas id, track_id e img
    df = df.drop(columns=['id', 'track_id', 'img'])
    
    # Guardar en CSV en la carpeta de Downloads
    df.to_csv(DOWNLOAD_PATH, index=False)
    logging.info(f"CSV exportado a {DOWNLOAD_PATH}")
    
    connection.close()

# Definición del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    dag_id='export_table_to_csv',  # Nombre del nuevo DAG
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Tarea para eliminar columnas y exportar CSV
    eliminar_columnas_task = PythonOperator(
        task_id='eliminar_columnas_y_exportar',
        python_callable=eliminar_columnas_y_exportar
    )

eliminar_columnas_task
