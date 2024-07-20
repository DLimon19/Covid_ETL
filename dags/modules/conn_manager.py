import pandas as pd
import psycopg2
import json

from sqlalchemy import create_engine
from datetime import datetime

# Funcion para la conexion a la base de datos    
def get_conn(**kwargs):
    print(f"Conectandose a la BD en la fecha: {kwargs["exec_date"]}") 
    try:
        conn = psycopg2.connect(
            host = kwargs["config"]["host"],
            dbname = kwargs["config"]["database"],
            user = kwargs["config"]["username"],
            password = kwargs["config"]["pwd"],
            port = kwargs["config"]["port"])
        print(conn)
        print("Connected to Redshift successfully!")
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)

# Funcion para la carga a la base de datos
def upload_data(**kwargs):

    print(f"Cargando la data para la fecha: {kwargs["exec_date"]}")
    date = datetime.strptime(kwargs["exec_date"], '%Y-%m-%d %H')
    
    path_covid = kwargs["dag_path"]+'/raw_data/'+"data_covid_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json"

    with open(path_covid, "r") as json_file:
        loaded_covid_data=json.load(json_file)
    records_covid = pd.json_normalize(loaded_covid_data)

    path_paises = kwargs["dag_path"]+'/raw_data/'+"data_paises_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json"

    with open(path_paises, "r") as json_file:
        loaded_paises_data=json.load(json_file)

    list_paises = [i["name"]["common"] for i in loaded_paises_data]

    filtered_data = records_covid[records_covid["Country_Region"].isin(list_paises)]

    host = kwargs["config"]["host"]
    dbname = kwargs["config"]["database"]
    username = kwargs["config"]["username"]
    password = kwargs["config"]["pwd"]
    port = kwargs["config"]["port"]
    
    connection_url = f"redshift+psycopg2://{username}:{password}@{host}:{port}/{dbname}"
    db_engine = create_engine(connection_url)
    
    try:
        filtered_data.to_sql(
            kwargs["table"],
            con=db_engine,
            schema=kwargs["schema"],
            if_exists='replace',
            index=False
        )

        print(f"Data from the DataFrame has been uploaded to the {kwargs["schema"]}.{kwargs["table"]} table in Redshift.")
    except Exception as e:
        print(f"Failed to upload data to {kwargs["schema"]}.{kwargs["table"]}:\n{e}")
        raise
    finally:
        if db_engine:
            db_engine.dispose()
            print("Connection to Redshift closed.")
        else:
            print("No active connection to close.")



