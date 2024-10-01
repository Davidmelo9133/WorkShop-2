from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import mysql.connector
import pandas as pd
from datetime import datetime
import logging

def conectar_mysql():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='password',  # Cambia tu contraseña aquí
        database='spotify_data'
    )

def clean_and_outer_join():
    logging.info("Iniciando la función clean_and_outer_join")
    connection = conectar_mysql()
    
    try:
        # Cargar los datos desde MySQL
        query_spotify = "SELECT * FROM Spotify"
        query_grammy = "SELECT * FROM Grammy_awards"
        
        spotify_df = pd.read_sql(query_spotify, connection)
        grammy_df = pd.read_sql(query_grammy, connection)
        
        logging.info(f"Datos cargados - Spotify: {len(spotify_df)} filas, Grammy: {len(grammy_df)} filas")

        # Realizar un outer join
        merged_df = pd.merge(spotify_df, grammy_df, how='outer', left_on='track_name', right_on='title')
        logging.info(f"Merge completado - Resultado: {len(merged_df)} filas")

        # Reemplazar NaN con valores por defecto
        merged_df = merged_df.fillna({
            'id': 0,
            'track_id': '',
            'artists': '',
            'album_name': '',
            'track_name': '',
            'popularity': 0,
            'duration_ms': 0,
            'explicit': False,
            'danceability': 0.0,
            'energy': 0.0,
            'key': 0,
            'loudness': 0.0,
            'mode': 0,
            'speechiness': 0.0,
            'acousticness': 0.0,
            'instrumentalness': 0.0,
            'liveness': 0.0,
            'valence': 0.0,
            'tempo': 0.0,
            'time_signature': 0,
            'track_genre': 'Unknown',
            'year': 0,
            'title': '',
            'published_at': datetime(2000, 1, 1),
            'updated_at': datetime(2000, 1, 1),
            'category': '',
            'nominee': '',
            'artist': '',
            'workers': '',
            'img': '',
            'winner': False
        })

        # Asegurar tipos de datos correctos
        int_columns = ['id', 'popularity', 'duration_ms', 'key', 'mode', 'time_signature', 'year']
        float_columns = ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo']
        bool_columns = ['explicit', 'winner']
        
        for col in int_columns:
            merged_df[col] = merged_df[col].astype(int)
        for col in float_columns:
            merged_df[col] = merged_df[col].astype(float)
        for col in bool_columns:
            merged_df[col] = merged_df[col].astype(bool)

        # Crear la tabla combinada en MySQL
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Spotify_Grammy_Joined (
                id INT,
                track_id VARCHAR(50),
                artists TEXT,
                album_name TEXT,
                track_name TEXT,
                popularity INT,
                duration_ms INT,
                explicit BOOLEAN,
                danceability FLOAT,
                energy FLOAT,
                `key` INT,
                loudness FLOAT,
                mode INT,
                speechiness FLOAT,
                acousticness FLOAT,
                instrumentalness FLOAT,
                liveness FLOAT,
                valence FLOAT,
                tempo FLOAT,
                time_signature INT,
                track_genre VARCHAR(50),
                year INT,
                title TEXT,
                published_at DATETIME,
                updated_at DATETIME,
                category TEXT,
                nominee TEXT,
                artist TEXT,
                workers TEXT,
                img TEXT,
                winner BOOLEAN
            );
        """)
        logging.info("Tabla Spotify_Grammy_Joined creada o ya existente")

        # Limpiar la tabla antes de insertar nuevos datos
        cursor.execute("TRUNCATE TABLE Spotify_Grammy_Joined")
        logging.info("Tabla Spotify_Grammy_Joined limpiada")

        # Insertar los datos combinados en la nueva tabla
        insert_query = """
            INSERT INTO Spotify_Grammy_Joined (
                id, track_id, artists, album_name, track_name, popularity, duration_ms, explicit,
                danceability, energy, `key`, loudness, mode, speechiness, acousticness,
                instrumentalness, liveness, valence, tempo, time_signature, track_genre,
                year, title, published_at, updated_at, category, nominee, artist, workers, img, winner
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Convertir las filas del DataFrame en tuplas y evitar valores inconsistentes
        insert_data = merged_df.astype(object).where(pd.notnull(merged_df), None).values.tolist()
        cursor.executemany(insert_query, insert_data)
        logging.info(f"Insertadas {len(insert_data)} filas en Spotify_Grammy_Joined")

        connection.commit()
        logging.info("Transacción completada")
    except Exception as e:
        logging.error(f"Error durante la ejecución: {str(e)}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
        logging.info("Conexión cerrada")

# Definición del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG('spotify_grammy_outer_join', default_args=default_args, schedule_interval=None) as dag:

    # Definir la tarea para combinar las tablas
    clean_and_join_task = PythonOperator(
        task_id='clean_and_outer_join',
        python_callable=clean_and_outer_join
    )

clean_and_join_task