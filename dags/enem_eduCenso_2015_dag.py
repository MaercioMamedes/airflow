from airflow import DAG
from airflow.models import XCom, Variable
from airflow.decorators import task
import pendulum #usada para definir uma data especifica.
import datetime
import os
from os.path import join
import pandas as pd
import numpy as np


with DAG(
    "Dados_censo_enem",
    start_date=pendulum.datetime(2023, 6, 3, tz="UTC"),

    schedule_interval='0 0 * * 6',    
) as dag:
    
    @task(task_id = "leitura_dados_censo_2015")
    def tarefa_1(**context):
        dataset_censo_interno = pd.read_csv("dags/microdados_ed_basica_2015.csv", encoding="latin", sep=";", low_memory=False)
        context['ti'].xcom_push(key='dataset_senso', value=dataset_censo_interno)
    
    @task(task_id = "leitura_notas_enem")
    def tarefa_2(**context):
        dataset_enem_interno = pd.read_csv("dags/MICRODADOS_ENEM_ESCOLA.csv", encoding="latin", sep=";", low_memory=False)
        context['ti'].xcom_push(key='dataset_enem', value=dataset_enem_interno)
        
        
    
    @task(task_id='Limpeza_de_colunas')
    def tarefa_3(**context):
        #context['ti'].xcom_pull(key='dataset_enem')
        dataset_censo = context['ti'].xcom_pull(key='dataset_censo', task_id="leitura_dados_censo_2015")
        dataset_enem = context['ti'].xcom_pull(key='dataset_enem', task_id="leitura_notas_enem")
        column_used_from_dataset_enem = [ 
    'NU_ANO', 'CO_ESCOLA_EDUCACENSO',  'NU_MATRICULAS', 'NU_PARTICIPANTES_NEC_ESP', 'NU_PARTICIPANTES', 'NU_TAXA_PARTICIPACAO', 
    'NU_MEDIA_CN', 'NU_MEDIA_CH', 'NU_MEDIA_LP', 'NU_MEDIA_MT','NU_MEDIA_RED', 'NU_MEDIA_OBJ', 'NU_MEDIA_TOT', 'NU_TAXA_APROVACAO', 
    'NU_TAXA_REPROVACAO',  'NU_TAXA_ABANDONO' 
    ]

        columns_used_from_dataset_censo = [  
    'NU_ANO_CENSO', 'SG_UF', 'CO_ENTIDADE', 'NO_ENTIDADE','NO_MUNICIPIO','CO_MUNICIPIO', 'TP_DEPENDENCIA',
    'TP_CATEGORIA_ESCOLA_PRIVADA', 'TP_LOCALIZACAO', 'TP_SITUACAO_FUNCIONAMENTO', 'DT_ANO_LETIVO_INICIO',
    'DT_ANO_LETIVO_TERMINO', 'TP_CONVENIO_PODER_PUBLICO', 'TP_REGULAMENTACAO', 'TP_RESPONSAVEL_REGULAMENTACAO',
    'IN_AUDITORIO', 'IN_LABORATORIO_INFORMATICA', 'IN_LABORATORIO_CIENCIAS', 'IN_BIBLIOTECA_SALA_LEITURA'
    ]

      
        dataset_censo = dataset_censo.filter(items=columns_used_from_dataset_censo) 
        dataset_enem = dataset_enem.filter(items=column_used_from_dataset_enem)
        #return 'Datasets filtrados com sucesso'
        return dataset_censo, dataset_enem
    
    @task(task_id='alterando_tipos_de_dados')
    def tarefa_4(**context):
        # Passo 2 - Alterando os tipos de dados.
        # Alterando o tipo da coluna DT_ANO_LETIVO_INICIO e DT_ANO_LETIVO_TERMINO para datetime
        dataset_censo['DT_ANO_LETIVO_INICIO'] = dataset_censo['DT_ANO_LETIVO_INICIO'].replace('0', '02FEB2015')
        dataset_censo['DT_ANO_LETIVO_INICIO'] = dataset_censo['DT_ANO_LETIVO_INICIO'].replace(':00:00:00$', '', regex=True)
        dataset_censo['DT_ANO_LETIVO_INICIO'] = pd.to_datetime(dataset_censo['DT_ANO_LETIVO_INICIO'], format='%d%b%Y')
        dataset_censo['DT_ANO_LETIVO_INICIO'] = dataset_censo['DT_ANO_LETIVO_INICIO'].dt.strftime('%Y-%m-%d')

        dataset_censo['DT_ANO_LETIVO_TERMINO'] = dataset_censo['DT_ANO_LETIVO_TERMINO'].replace('0', np.nan)
        dataset_censo['DT_ANO_LETIVO_TERMINO'] = dataset_censo['DT_ANO_LETIVO_TERMINO'].replace(':00:00:00$', '', regex=True)
        dataset_censo['DT_ANO_LETIVO_TERMINO'] = dataset_censo['DT_ANO_LETIVO_TERMINO'].replace(':01:00:00$', '', regex=True)
        dataset_censo['DT_ANO_LETIVO_TERMINO'] = pd.to_datetime(dataset_censo['DT_ANO_LETIVO_TERMINO'], format='%d%b%Y')
        dataset_censo['DT_ANO_LETIVO_TERMINO'] = dataset_censo['DT_ANO_LETIVO_TERMINO'].dt.strftime('%Y-%m-%d')


    @task(task_id='tokenizando_dados')
    def tarefa_5():
        # Passo 3 - Tokenizando os dados.
        global dataset_censo
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


    @task(task_id='calculando_medias')
    def tarefa_6():
        # Passo 4 - Calculando a média NU_MEDIA_OBJ e NU_MEDIA_TOT.
        global dataset_enem
        media_objetiva = dataset_enem[['NU_MEDIA_CN', 'NU_MEDIA_CH', 'NU_MEDIA_LP', 'NU_MEDIA_MT']].mean(axis=1)
        dataset_enem['NU_MEDIA_OBJ'] = round(media_objetiva, 2)

        media_total = dataset_enem[['NU_MEDIA_CN', 'NU_MEDIA_CH', 'NU_MEDIA_LP', 'NU_MEDIA_MT', 'NU_MEDIA_RED']].mean(axis=1)
        dataset_enem['NU_MEDIA_TOT'] = round(media_objetiva, 2)

    
    @task(task_id='filtro_ano_enem')
    def tarefa_7():
        # Passo 5 - Mudando o nome da coluna de referência com a tabela do censo.
        global dataset_enem
        dataset_enem = dataset_enem.rename(columns={'CO_ESCOLA_EDUCACENSO':'CO_ENTIDADE'})
        dataset_enem = dataset_enem.loc[dataset_enem['NU_ANO'] == 2015]

    @task(task_id='merge_dos_dados')
    def tarefa_8():
        # Passo 7 - Transformando em dataframe
        global dataset_censo
        global dataset_enem

        df_censo = pd.DataFrame(dataset_censo)
        df_enem = pd.DataFrame(dataset_enem)

        # Passo 8 - Fazendo o merge dos datasets

        dataset_censo_enem = pd.merge(df_censo, df_enem,on='CO_ENTIDADE' )

        # Salvando o arquivo trabalhado em *.csv

        dataset_censo_enem.to_csv('./novos_arquivos/dataset_censo_enem.csv', sep=';', encoding="UTF-8")

    tarefa_1() >> tarefa_2() >> tarefa_3() >> tarefa_4() >> \
    tarefa_5() >> tarefa_6() >> tarefa_7() >> tarefa_8()