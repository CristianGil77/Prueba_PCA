from datetime import datetime, timedelta
from pytz import timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

# Definir las rutas de los archivos
costos_path = "/opt/airflow/data/costos.xlsx"
ventas_path = "/opt/airflow/data/ventas.xlsx"

# Función para leer costos
def extract_costos(**kwargs):
    costos_df = pd.read_excel(costos_path)
    # Guardar en archivo temporal
    costos_df.to_csv('/tmp/costos_tmp.csv', index=False)

# Función para leer ventas
def extract_ventas(**kwargs):
    ventas_df = pd.read_excel(ventas_path)
    # Guardar en archivo temporal
    ventas_df.to_csv('/tmp/ventas_tmp.csv', index=False)

def create_dim_tiempo(**kwargs):
    anio_actual = datetime.now().year
    anios = range(2024, anio_actual + 1)
    meses = range(1, 13)
    tiempo_data = []

    for año in anios:
        for mes in meses:
            mes_nombre = pd.to_datetime(f"{año}-{mes}-01").strftime("%B")
            trimestre = (mes - 1) // 3 + 1
            año_mes = int(f"{año}{str(mes).zfill(2)}")
            tiempo_data.append({
                'AñoMes': año_mes,
                'Año': año,
                'Mes': mes,
                'Mes Nombre': mes_nombre,
                'Trimestre': trimestre
            })

    dim_tiempo = pd.DataFrame(tiempo_data)
    
    # Enviar el DataFrame a la siguiente tarea
    return dim_tiempo.to_dict(orient='list')

# Función para transformar los datos
def transform_data(**kwargs):
    # Recuperar los DataFrames pasados de las tareas previas
    costos_df = pd.read_csv('/tmp/costos_tmp.csv')
    ventas_df = pd.read_csv('/tmp/ventas_tmp.csv')

    # Transformación de costos
    fact_costos = pd.melt(costos_df, id_vars=['Año', 'Empresa'],
                          value_vars=['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
                                      'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'],
                          var_name='Mes Nombre', value_name='Costo')

    mes_map = {'Enero': '01', 'Febrero': '02', 'Marzo': '03', 'Abril': '04', 'Mayo': '05', 'Junio': '06',
               'Julio': '07', 'Agosto': '08', 'Septiembre': '09', 'Octubre': '10', 'Noviembre': '11', 'Diciembre': '12'}
    fact_costos['Mes'] = fact_costos['Mes Nombre'].map(mes_map)
    fact_costos['AñoMes'] = fact_costos['Año'].astype(str) + fact_costos['Mes']
    fact_costos['AñoMes'] = fact_costos['AñoMes'].astype(int)
    fact_costos = fact_costos.groupby(['AñoMes', 'Empresa'], as_index=False)['Costo'].sum()

    # Transformación de ventas
    ventas_df['Fecha contabilizacion'] = pd.to_datetime(ventas_df['Fecha contabilizacion'])
    ventas_df['Año'] = ventas_df['Fecha contabilizacion'].dt.year
    ventas_df['Mes'] = ventas_df['Fecha contabilizacion'].dt.month
    ventas_df['AñoMes'] = ventas_df['Año'].astype(str) + ventas_df['Mes'].apply(lambda x: f"{x:02d}")
    ventas_df['AñoMes'] = ventas_df['AñoMes'].astype(int)
    fact_ventas = ventas_df.groupby(['AñoMes', 'Empresa'], as_index=False)['Acumulado Ventas'].sum()

    # Dimensión de empresas
    empresas_unicas = pd.concat([fact_costos['Empresa'], fact_ventas['Empresa']]).unique()
    dim_empresa = pd.DataFrame({'Empresa': empresas_unicas})

    # Retornar los DataFrames resultantes para la siguiente tarea
    return {
        'fact_costos': fact_costos.to_dict(orient='list'),
        'fact_ventas': fact_ventas.to_dict(orient='list'),
        'dim_empresa': dim_empresa.to_dict(orient='list')
    }

# Función para cargar los datos transformados en un archivo Excel
def load_data(ti, **kwargs):
    # Recuperar los datos enviados desde las otras tareas usando XCom
    dim_tiempo_dict = ti.xcom_pull(task_ids='create_dim_tiempo')
    transform_data_dict = ti.xcom_pull(task_ids='transform_data')

    # Convertir los diccionarios a DataFrames
    dim_tiempo = pd.DataFrame(dim_tiempo_dict)
    fact_costos = pd.DataFrame(transform_data_dict['fact_costos'])
    fact_ventas = pd.DataFrame(transform_data_dict['fact_ventas'])
    dim_empresa = pd.DataFrame(transform_data_dict['dim_empresa'])

    # Guardar en un archivo Excel con varias hojas
    with pd.ExcelWriter('/opt/airflow/data/Datos_Modelo.xlsx') as writer:
        dim_tiempo.to_excel(writer, sheet_name='Dim_Tiempo', index=False)
        fact_costos.to_excel(writer, sheet_name='Fact_Costos', index=False)
        fact_ventas.to_excel(writer, sheet_name='Fact_Ventas', index=False)
        dim_empresa.to_excel(writer, sheet_name='Dim_Empresa', index=False)

    print("Archivo Excel corregido generado con éxito.")


# Argumentos predeterminados para las tareas del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  
    'retries': 2,  
    'retry_delay': timedelta(minutes=15),  
}

# Crear el DAG
with DAG(
    'etl_costos_ventas',
    default_args=default_args,
    schedule_interval='0 8 * * *',  # Ejecutar diariamente a las 8:00 AM
    catchup=False
) as dag:

    # Tarea de extracción de costos
    extract_costos_task = PythonOperator(
        task_id='extract_costos',
        python_callable=extract_costos,
    )

    # Tarea de extracción de ventas
    extract_ventas_task = PythonOperator(
        task_id='extract_ventas',
        python_callable=extract_ventas,
    )

    # Tarea de extracción de ventas
    create_dim_tiempo_task = PythonOperator(
        task_id='create_dim_tiempo',
        python_callable=create_dim_tiempo,
    )

    # Tarea de transformación de datos
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    # Tarea de carga de datos
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    # Flujo de trabajo
    [extract_costos_task, extract_ventas_task] >> transform_task
    transform_task >> load_task
    create_dim_tiempo_task >> load_task