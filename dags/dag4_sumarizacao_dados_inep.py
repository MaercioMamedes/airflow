from airflow import DAG
from airflow.decorators import task
import pendulum #usada para definir uma data especifica.
from os.path import join
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def get_engine_database(database):
    engine = create_engine(f"postgresql://postgres:postgres@0.0.0.0:5436/{database}")
    return engine

with DAG(
    "sumarizacao_dados_INEP",
    start_date=pendulum.datetime(2023, 6, 3, tz="UTC"),
    tags=["censo_enem","sumarizacao"],
    schedule_interval='0 0 * * 6',    
) as dag:
    
   

    @task(task_id='merge_dos_dados')
    def tarefa_1():
        # Merge e persistência dos dados
        dataset_censo = pd.read_sql_query('SELECT * FROM censo_2015_clean', con=get_engine_database('database_censo_enem_clean'))
        dataset_enem = pd.read_sql_query('SELECT * FROM notas_enem', con=get_engine_database('database_censo_enem_clean'))

        df_censo = pd.DataFrame(dataset_censo)
        df_enem = pd.DataFrame(dataset_enem)

        # Fazendo o merge dos datasets
        dataset_censo_enem = pd.merge(df_censo, df_enem,on='CO_ENTIDADE' )

        # Salvando em banco de dados
        dataset_censo_enem.to_sql('dados_inep_merged', get_engine_database('censo_enem_summarized'))

        return "Dados sumarizdos e salvos com sucesso"

    tarefa_1()