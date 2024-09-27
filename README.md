# Proyecto RPA con Airflow

Este proyecto fue desarrollado como una prueba para crear un RPA usando Airflow, con el propósito de automatizar la **extracción**, **transformación** y **carga** de los datos suministrados.

## Descripción

- El **DAG** ha sido configurado para ejecutarse diariamente.
- El script con la definición del DAG se encuentra en la siguiente ruta:

  [airflow-docker/dags](./airflow-docker/dags)

- Los datos suministrados de ventas y costos se encuentran en:

  [airflow-docker/data](./airflow-docker/data)

- Después de que el DAG se ejecuta con éxito, el archivo generado con los resultados, **Datos_Modelo.xlsx**, se almacena en:

  [airflow-docker/data/Datos_Modelo.xlsx](./airflow-docker/data/Datos_Modelo.xlsx)

## Cómo ejecutar

Asegúrate de que Airflow está correctamente configurado y en funcionamiento dentro de Docker.

```bash
docker-compose up airflow-init
docker-compose up
````
