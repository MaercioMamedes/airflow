from airflow import DAG
from airflow.decorators import task
import pendulum #usada para definir uma data especifica.
from os.path import join
import pandas as pd
from sqlalchemy import create_engine


def get_engine_database():
    engine = create_engine("postgresql://postgres:postgres@0.0.0.0:5436/database_censo_enem")
    return engine

with DAG(
    "Limpeza_dados_ENEM",
    start_date=pendulum.datetime(2023, 6, 3, tz="UTC"),
    tags=["censo_enem","Limpeza_dados"],
    schedule_interval='0 0 * * 6',    
) as dag:
            
    @task(task_id='Limpeza_de_colunas')
    def tarefa_1():
    # Filtro das colunas utilizadas
        dataset_enem = pd.DataFrame() # realizar pull no banco de dados

        column_used_from_dataset_enem = [ 
            'NU_ANO', 'CO_ESCOLA_EDUCACENSO',
            'NU_MATRICULAS', 
            'NU_PARTICIPANTES_NEC_ESP', 
            'NU_PARTICIPANTES', 
            'NU_TAXA_PARTICIPACAO', 
            'NU_MEDIA_CN', 
            'NU_MEDIA_CH', 
            'NU_MEDIA_LP', 
            'NU_MEDIA_MT',
            'NU_MEDIA_RED', 
            'NU_MEDIA_OBJ', 
            'NU_MEDIA_TOT', 
            'NU_TAXA_APROVACAO', 
            'NU_TAXA_REPROVACAO',
            'NU_TAXA_ABANDONO', 
    ]
        dataset_enem = dataset_enem.filter(items=column_used_from_dataset_enem)
        dataset_enem = dataset_enem.rename(columns={'CO_ESCOLA_EDUCACENSO':'CO_ENTIDADE'})
        dataset_enem = dataset_enem.loc[dataset_enem['NU_ANO'] == 2015]
        
        return "Colunas da Tabela Enem filtradas"


    @task(task_id='calculando_medias')
    def tarefa_2():

        # Calculando a média NU_MEDIA_OBJ e NU_MEDIA_TOT.
        global dataset_enem
        media_objetiva = dataset_enem[['NU_MEDIA_CN', 'NU_MEDIA_CH', 'NU_MEDIA_LP', 'NU_MEDIA_MT']].mean(axis=1)
        dataset_enem['NU_MEDIA_OBJ'] = round(media_objetiva, 2)

        media_total = dataset_enem[['NU_MEDIA_CN', 'NU_MEDIA_CH', 'NU_MEDIA_LP', 'NU_MEDIA_MT', 'NU_MEDIA_RED']].mean(axis=1)
        dataset_enem['NU_MEDIA_TOT'] = round(media_objetiva, 2)

    
    @task(task_id='persistindo_dados_enem')
    def tarefa_3():
        pass

    tarefa_1() >> tarefa_2() >> tarefa_3()
    