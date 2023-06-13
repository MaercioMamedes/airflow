from airflow import DAG
from airflow.decorators import task
import pendulum #usada para definir uma data especifica.
from os.path import join
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def get_engine_database():
    engine = create_engine("postgresql://postgres:postgres@0.0.0.0:5436/database_censo_enem")
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
        global dataset_censo
        global dataset_enem

        df_censo = pd.DataFrame(dataset_censo)
        df_enem = pd.DataFrame(dataset_enem)

        # Passo 8 - Fazendo o merge dos datasets

        dataset_censo_enem = pd.merge(df_censo, df_enem,on='CO_ENTIDADE' )

        # Salvando o arquivo trabalhado em *.csv

        dataset_censo_enem.to_csv('./novos_arquivos/dataset_censo_enem.csv', sep=';', encoding="UTF-8")

    tarefa_1()