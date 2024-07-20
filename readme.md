# Proyecto Covid ETL

Este es un proyecto para el curso de Data Engineer de la plataforma de Coderhouse.

Es un proceso de ETL que extrae la informacion de los casos de COVID de distintos paises.
En este paso tomando 2 fuentes de informacion para realizar un filtro donde se toman a consideracion
2 API, una de los casos de COVID y otra que en este caso devuelve los nombre de los paises del mundo
que hablan espa;ol

### Fuentes de informacion:

- [HTTP API for Latest Covid-19 Data](https://pipedream.com/@pravin/http-api-for-latest-covid-19-data-p_G6CLVM/readme)
- [REST COUNTRIES](https://restcountries.com/)

### Consideraciones

Antes de ejecutar el proyecto es necesario realizar algunos cambios para la configuracion del
smtp para el envio del correo

#### airflow.cfg

Este caso es para utilizar un correo de Gmail. Antes de modificar el archivo sera necesario seguir los siguientes pasos

Cree una contraseña de aplicación de Google para su cuenta de Gmail (instrucciones aquí). Esto se hace para que no utilice su contraseña original o la autenticación de 2 factores.

- Visita la página de contraseñas de tu aplicación. Es posible que se le solicite que inicie sesión en su cuenta de Google.
- En "Las contraseñas de tus aplicaciones", haz clic en Seleccionar aplicación y elige "Correo".
- Haga clic en Seleccionar dispositivo y elija "Otro (nombre personalizado)" para poder ingresar "Flujo de aire". 
- Seleccione Generar.
- Copie la contraseña de la aplicación generada (el código de 16 caracteres en la barra amarilla), por ejemplo xxxxyyyyxxxxyyyy.
- Seleccione Listo.

Una vez que haya terminado, no volverá a ver el código de contraseña de la aplicación. Sin embargo, verá una lista de aplicaciones y dispositivos (para los cuales ha creado contraseñas de aplicaciones) en su cuenta de Google.

Edite ```airflow.cfg``` y edite la sección ```[smtp]``` como se muestra a continuación:

```
[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = YOUR_EMAIL_ADDRESS
smtp_password = xxxxyyyyxxxxyyyy
smtp_port = 587
smtp_mail_from = YOUR_EMAIL_ADDRESS
```

Adicionalmente colocar las credenciales necesarias en el archivo .env para la conexion a la base de datos.

## Ejecucion

- Create of folders and download yml
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.2/docker-compose.yaml'

mkdir -p ./{logs,dags,config,plugins,raw_data}

echo -e "AIRFLOW_UID=$(id -u)" >> ./.env


```

- Copy .env to dags folder
```bash
cp .env ./dags/
```


- Start project
```bash
docker compose up airflow-init
```
```bash
docker compose up
```
