from airflow import DAG
from airflow.decorators import task
import pendulum #usada para definir uma data especifica.
import pandas as pd
from sqlalchemy import create_engine


def get_engine_database():
    engine = create_engine("postgresql://postgres:postgres@0.0.0.0:5436/database_censo_enem")
    return engine

with DAG(

    
    "Extracao_censo_enem_2015",
    start_date=pendulum.datetime(2023, 6, 3, tz="UTC"),
    tags=["censo_enem","Extracao_Dados_INEP"],
    schedule_interval='0 0 * * 6',    
) as dag:
    
    @task(task_id = "leitura_dados_censo_2015")
    def tarefa_1(**context):
        # Extrair dados do CENSO escolar e persistir num banco de dados

        dataset_censo = pd.read_csv("./dags/csv_files/microdados_ed_basica_2015.csv", encoding="latin", sep=";", low_memory=False)
        df_censo = pd.DataFrame(dataset_censo)
        df_censo.to_sql('censo_2015', get_engine_database())
        return 'banco de dados criado com sucesso'
    
    @task(task_id = "leitura_notas_enem")
    def tarefa_2(**context):
        # Extrair dados das Notas do ENEM e persistir num banco de dados
        dataset_enem_interno = pd.read_csv("dags/MICRODADOS_ENEM_ESCOLA.csv", encoding="latin", sep=";", low_memory=False)
        context['ti'].xcom_push(key='dataset_enem', value=dataset_enem_interno)
        

    tarefa_1() >> tarefa_2() 
    
