# Prueba Técnica PCA Ingeniería

Como parte de esta prueba, se desarrolló un RPA utilizando **Apache Airflow** para automatizar la **extracción**, **transformación** y **carga** diaria de los datos suministrados. Los resultados de este proceso se visualizan posteriormente en un dashboard de **Power BI**.

## Descripción

Este repositorio incluye tanto el contenedor Docker de Airflow como el dashboard de Power BI.

- El DAG de Airflow está configurado para ejecutarse diariamente, y en caso de fallo, realizará hasta dos intentos adicionales con un intervalo de 15 minutos entre cada uno. El script que define el DAG se encuentra en la siguiente ruta:

  [airflow-docker/dags](./airflow-docker/dags)

- Los datos de ventas y costos proporcionados se almacenan en:

  [airflow-docker/data](./airflow-docker/data)

- Al finalizar exitosamente la ejecución del DAG, el archivo generado con los resultados, **Datos_Modelo.xlsx**, se guarda en la siguiente ubicación:

  [airflow-docker/data/Datos_Modelo.xlsx](./airflow-docker/data/Datos_Modelo.xlsx)

## Instrucciones para ejecutar

Para ejecutar el DAG, sigue estos pasos tras clonar este repositorio:

1. Accede al directorio `airflow-docker`.
2. Inicia el entorno de Airflow desde docker.

   ```bash
   cd airflow-docker
   docker-compose up airflow-init
   docker-compose up
   ```
Una vez que la aplicación esté en funcionamiento, podrá acceder al panel de control en [http://localhost:8080](http://localhost:8080) utilizando las siguientes credenciales:

- **Usuario**: `airflow`
- **Contraseña**: `airflow`

En la pestaña "DAGs", encontrará el flujo de trabajo llamado **etl_costos_ventas**.
