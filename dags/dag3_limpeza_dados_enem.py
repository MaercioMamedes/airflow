from airflow import DAG
from airflow.decorators import task
import pendulum #usada para definir uma data especifica.
from os.path import join
import pandas as pd
from sqlalchemy import create_engine


def get_engine_database(database):
    engine = create_engine(f"postgresql://postgres:postgres@0.0.0.0:5436/{database}")
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

        dataset_enem = pd.read_sql_query('SELECT * FROM notas_enem', con=get_engine_database('database_censo_enem_clean'))

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

        # mudando nome da coluna com relacionamento para terem o mesmo nome
        dataset_enem = dataset_enem.rename(columns={'CO_ESCOLA_EDUCACENSO':'CO_ENTIDADE'})

        # filtrando as notas do ano de 2015
        dataset_enem = dataset_enem.loc[dataset_enem['NU_ANO'] == 2015]
        
        # Salvando em banco de dados
        df_enem = pd.DataFrame(dataset_enem)
        df_enem.to_sql('notas_enem', get_engine_database('database_censo_enem_clean'), if_exists='replace')

        return "Colunas da Tabela Enem filtradas"


    @task(task_id='calculando_medias')
    def tarefa_2():
        dataset_enem = pd.read_sql_query('SELECT * FROM notas_enem', con=get_engine_database('database_censo_enem_clean'))

        # calculando médias das provas objetivas
        media_objetiva = dataset_enem[['NU_MEDIA_CN', 'NU_MEDIA_CH', 'NU_MEDIA_LP', 'NU_MEDIA_MT']].mean(axis=1)
        dataset_enem['NU_MEDIA_OBJ'] = round(media_objetiva, 2)

        # calculando média total
        media_total = dataset_enem[['NU_MEDIA_CN', 'NU_MEDIA_CH', 'NU_MEDIA_LP', 'NU_MEDIA_MT', 'NU_MEDIA_RED']].mean(axis=1)
        dataset_enem['NU_MEDIA_TOT'] = round(media_total, 2)

        # salvando em banco de dados
        df_enem = pd.DataFrame(dataset_enem)
        df_enem.to_sql('notas_enem', get_engine_database('database_censo_enem_clean'), if_exists='replace')

        return 'Médias calculadas e salvas com sucesso'



    tarefa_1() >> tarefa_2()
    