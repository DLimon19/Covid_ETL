from datetime import timedelta,datetime
from pathlib import Path
from dotenv import load_dotenv
from airflow import DAG

from modules import get_data, get_conn, upload_data, executeSqlFromFile

# Operadores
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import os


dag_path = os.getcwd()     #path original.. home en Docker

load_dotenv()  # take environment variables from .env.

url = os.getenv("REDSHIFT_HOST")
user = os.getenv("REDSHIFT_USERNAME")
pwd = os.getenv("REDSHIFT_PASSWORD")
data_base = os.getenv("REDSHIFT_DBNAME")
port = os.getenv("REDSHIFT_PORT")

schema = "luis_981908_coderhouse"
table = "stage_covid_data"
filename = 'ddl/sql_data.sql'

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': port,
    'pwd': pwd
}

# argumentos por defecto para el DAG
default_args = {
    'owner': 'danielrdz',
    'start_date': datetime(2024,7,15),
    'email': ['luis_981908@hotmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

ETL_dag = DAG(
    dag_id='Covid_ETL',
    default_args=default_args,
    description='Agrega data de casos de Covid por estados de cada pais',
    schedule_interval="@once",
    catchup=False
)

dag_path = os.getcwd()     #path original.. home en Docker

# Tareas
# 1. Extraccion de informacion
task_1 = PythonOperator(
    task_id='crear_tabla',
    python_callable=executeSqlFromFile,
    op_kwargs={"exec_date":"{{ ds }} {{ execution_date.hour }}","filename":filename, "url": url , "config":redshift_conn, "dag_path":dag_path},
    dag=ETL_dag,
)

# 1. Extraccion de informacion
task_2 = PythonOperator(
    task_id='extraer_data',
    python_callable=get_data,
    #op_args=["{{ ds }} {{ execution_date.hour }}",dag_path],
    op_kwargs={"exec_date":"{{ ds }} {{ execution_date.hour }}","dag_path":dag_path},
    dag=ETL_dag,
)

task_3 = PythonOperator(
    task_id='conexion_BD',
    python_callable=get_conn,
    #op_args=["{{ ds }} {{ execution_date.hour }}",redshift_conn],
    op_kwargs={"exec_date":"{{ ds }} {{ execution_date.hour }}","dag_path":dag_path},
    dag=ETL_dag,
)

task_4 = PythonOperator(
    task_id='cargar_data',
    python_callable=upload_data,
    #op_args=["{{ ds }} {{ execution_date.hour }}",dag_path,table,schema,redshift_conn],
    op_kwargs={"exec_date":"{{ ds }} {{ execution_date.hour }}","dag_path":dag_path,"table":table,"schema":schema,"config":redshift_conn},
    dag=ETL_dag,
)

email_success = EmailOperator(
        task_id='enviar_email',
        to='luis_981908@hotmail.com',
        subject='Airflow ETL exitoso',
        html_content= f"""
                    Hola, <br>
                    <p>Este es un mensaje de alerta</p> 
                    <p>El proceso ETL Covid termino exitosamente</p>
                    <br> Gracias. <br>
                """,
        dag=ETL_dag
)

# Definicion orden de tareas
task_1 >> task_2 >> task_3 >> task_4 >> email_success