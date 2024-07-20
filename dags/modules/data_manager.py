from datetime import datetime

import pandas as pd
import urllib.request
import json

# Se crea el metodo get_data para el llamado a la API
def get_data(**kwargs):
    try:
        print(f"Adquiriendo data para la fecha: {kwargs["exec_date"]}")
        date = datetime.strptime(kwargs["exec_date"], '%Y-%m-%d %H')
        # Se hace el request a la API
        response_covid = urllib.request.urlopen("https://coronavirus.m.pipedream.net/").read()
        if response_covid:
            print('Llamada a la API de datos COVID exitosa!')
            # Se convierte la respuesta en un json
            data_covid = json.loads(response_covid)
            # Se crea un archivo json en el directorio ./raw_data/ y se le cargan los datos
            with open(kwargs["dag_path"]+'/raw_data/'+"data_covid_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "w") as json_file:
                   json.dump(data_covid["rawData"], json_file)
        else:
            print('An error has occured!')

        response_paises = urllib.request.urlopen("https://restcountries.com/v3.1/lang/spanish").read()
        if response_paises:
            print('Llamada a la API Paises de habla Hispana exitosa!')
            # Se convierte la respuesta en un json
            data_paises = json.loads(response_paises)
            with open(kwargs["dag_path"]+'/raw_data/'+"data_paises_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "w") as json_file:
                   json.dump(data_paises, json_file)
        
    except Exception as e:
        #logging.error(e)
        print(e)
    finally:
        print("Check the data format")

def transform_data(**kwargs):
    print(f"Adquiriendo data para la fecha: {kwargs["exec_date"]}")
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