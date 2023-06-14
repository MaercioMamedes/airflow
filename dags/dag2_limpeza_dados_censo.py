from airflow import DAG
from airflow.decorators import task
import pendulum #usada para definir uma data especifica.
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def get_engine_database(database):
    engine = create_engine(f"postgresql://postgres:postgres@0.0.0.0:5436/{database}")
    return engine

with DAG(
    "Limpeza_dados_censo",
    start_date=pendulum.datetime(2023, 6, 3, tz="UTC"),
    tags=["censo_enem","Limpeza_dados"],
    schedule_interval='0 0 * * 6',    
) as dag:
            
    @task(task_id='Limpeza_de_colunas')
    def tarefa_1():
        # Filtro das colunas utilizadas pelo estudo 

        dataset_censo = pd.read_sql_query('SELECT * FROM censo_2015', con=get_engine_database('database_censo_enem'))

        columns_used_from_dataset_censo = [  
            'NU_ANO_CENSO', 'SG_UF', 
            'CO_ENTIDADE', 'NO_ENTIDADE',
            'NO_MUNICIPIO','CO_MUNICIPIO', 
            'TP_DEPENDENCIA',
            'TP_CATEGORIA_ESCOLA_PRIVADA', 
            'TP_LOCALIZACAO', 
            'TP_SITUACAO_FUNCIONAMENTO', 
            'DT_ANO_LETIVO_INICIO',
            'DT_ANO_LETIVO_TERMINO', 
            'TP_CONVENIO_PODER_PUBLICO', 
            'TP_REGULAMENTACAO', 
            'TP_RESPONSAVEL_REGULAMENTACAO',
            'IN_AUDITORIO', 
            'IN_LABORATORIO_INFORMATICA', 
            'IN_LABORATORIO_CIENCIAS', 
            'IN_BIBLIOTECA_SALA_LEITURA',
    ]

        dataset_censo = dataset_censo.filter(items=columns_used_from_dataset_censo) 
        df_censo = pd.DataFrame(dataset_censo)
        df_censo.to_sql('censo_2015_clean', get_engine_database('database_censo_enem_clean'))

        return 'Dados do censo limpados com sucesso'
    
    @task(task_id='alterando_tipos_de_dados')
    def tarefa_2():
        # Alterando o tipo da coluna DT_ANO_LETIVO_INICIO e DT_ANO_LETIVO_TERMINO para datetime

        dataset_censo = pd.read_sql_query('SELECT * FROM censo_2015_clean', con=get_engine_database('database_censo_enem_clean'))
        
        dataset_censo['DT_ANO_LETIVO_INICIO'] = dataset_censo['DT_ANO_LETIVO_INICIO'].replace('0', '02FEB2015')
        dataset_censo['DT_ANO_LETIVO_INICIO'] = dataset_censo['DT_ANO_LETIVO_INICIO'].replace(':00:00:00$', '', regex=True)
        dataset_censo['DT_ANO_LETIVO_INICIO'] = pd.to_datetime(dataset_censo['DT_ANO_LETIVO_INICIO'], format='%d%b%Y')
        dataset_censo['DT_ANO_LETIVO_INICIO'] = dataset_censo['DT_ANO_LETIVO_INICIO'].dt.strftime('%Y-%m-%d')

        dataset_censo['DT_ANO_LETIVO_TERMINO'] = dataset_censo['DT_ANO_LETIVO_TERMINO'].replace('0', np.nan)
        dataset_censo['DT_ANO_LETIVO_TERMINO'] = dataset_censo['DT_ANO_LETIVO_TERMINO'].replace(':00:00:00$', '', regex=True)
        dataset_censo['DT_ANO_LETIVO_TERMINO'] = dataset_censo['DT_ANO_LETIVO_TERMINO'].replace(':01:00:00$', '', regex=True)
        dataset_censo['DT_ANO_LETIVO_TERMINO'] = pd.to_datetime(dataset_censo['DT_ANO_LETIVO_TERMINO'], format='%d%b%Y')
        dataset_censo['DT_ANO_LETIVO_TERMINO'] = dataset_censo['DT_ANO_LETIVO_TERMINO'].dt.strftime('%Y-%m-%d')

        df_censo = pd.DataFrame(dataset_censo)
        df_censo.to_sql('censo_2015_clean', get_engine_database('database_censo_enem_clean'), if_exists='replace')

        return 'Realizada a limpeza dos valores de ano letivo com sucesso'

    @task(task_id='tokenizando_dados')
    def tarefa_3():
        # Tokenizando os dados.
        
        dataset_censo = pd.read_sql_query('SELECT * FROM censo_2015_clean', con=get_engine_database('database_censo_enem_clean'))
        
        dataset_censo['NO_MUNICIPIO'] = dataset_censo['NO_MUNICIPIO'].str.replace('[áãâàä]', 'a', regex=True)
        dataset_censo['NO_MUNICIPIO'] = dataset_censo['NO_MUNICIPIO'].str.replace('[ÁÃÂÀÄ]', 'A', regex=True)
        dataset_censo['NO_MUNICIPIO'] = dataset_censo['NO_MUNICIPIO'].str.replace('[éêèë]', 'e', regex=True)
        dataset_censo['NO_MUNICIPIO'] = dataset_censo['NO_MUNICIPIO'].str.replace('[ÉÊÈË]', 'E', regex=True)
        dataset_censo['NO_MUNICIPIO'] = dataset_censo['NO_MUNICIPIO'].str.replace('[íìîï]', 'i', regex=True)
        dataset_censo['NO_MUNICIPIO'] = dataset_censo['NO_MUNICIPIO'].str.replace('[ÍÌÎÏ]', 'I', regex=True)
        dataset_censo['NO_MUNICIPIO'] = dataset_censo['NO_MUNICIPIO'].str.replace('[óõôòö]', 'o', regex=True)
        dataset_censo['NO_MUNICIPIO'] = dataset_censo['NO_MUNICIPIO'].str.replace('[ÓÕÔÒÖ]', 'O', regex=True)
        dataset_censo['NO_MUNICIPIO'] = dataset_censo['NO_MUNICIPIO'].str.replace('[úùûü]', 'u', regex=True)
        dataset_censo['NO_MUNICIPIO'] = dataset_censo['NO_MUNICIPIO'].str.replace('[ÚÙÛÜ]', 'U', regex=True)
        dataset_censo['NO_MUNICIPIO'] = dataset_censo['NO_MUNICIPIO'].str.replace('[ç]', 'c', regex=True)

        df_censo = pd.DataFrame(dataset_censo)
        df_censo.to_sql('censo_2015_clean', get_engine_database('database_censo_enem_clean'), if_exists='replace')

        return 'Realizada limpeza no nome dos municípios'

    tarefa_1() >> tarefa_2() >> tarefa_3()
    