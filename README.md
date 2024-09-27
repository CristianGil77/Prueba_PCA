# Prueba Técnica PCA Ingeniería

Como parte de esta prueba, se desarrolló un RPA utilizando **Apache Airflow** para automatizar la **extracción**, **transformación** y **carga** diaria de los datos suministrados. Los resultados de este proceso se visualizan posteriormente en un dashboard de **Power BI**.

## Descripción

Este repositorio incluye tanto el contenedor Docker de Airflow como el dashboard de Power BI.

- El script con la definición del DAG se encuentra en la siguiente ruta:

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
