# Pipeline de engenharia de dados

## Utilizando a plataforma [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html) para construir um processo de extração, limepeza e sumarização de dados aberto do [INEP](https://www.gov.br/inep/pt-br/acesso-a-informacao/institucional/sobre)



### Apresentação

Esse projeto é o resultado de uma atividade avaliativa do curso de Pós Graduação em Engenharia de Software, no ano de 2023, do Centro Universitário CESMAC, sob orientação do Professor Dr Leonardo Fernandes Mendonça Oliveira.

O objetivo desse projeto é montar um pipeline de dados [ELT](https://kondado.com.br/blog/blog/2020/08/18/o-que-e-elt/) de duas base de dados do [INEP](https://www.gov.br/inep/pt-br/acesso-a-informacao/institucional/sobre), sendo uma o [Censo Escolar 2015](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar) e a outra é a base de dados das [Notas do Enem por escola de 2005 à 2015](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem-por-escola).

A motivação desse Pipeline é sumarizar uma base de dados estruturados, que possa responder algumas questões a respeito da influência da estrutura escolar, coletada pelo [Censo Escolar](https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/censo-escolar), no aproveitamento dos estudantes, no [Exame Nacional do Ensino Médio](https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/enem). 

### Autores

* [Maercio Mamedes da Silva](https://www.linkedin.com/in/maerciomamedes/)
* [Thiago de França Rodrigues](https://www.linkedin.com/in/thiago-de-fran%C3%A7a-rodrigues-b6375a27/)
  

### Etapas do Projeto

* Coleta dos dados do Censo Escolar do ano de 2015
* Coleta dos dados das Notas do Enem por escola
* Persistência dos dados brutos em banco de dados
* Limpeza das colunas não analisadas
* Limpeza/ajustes de dados não formatados
* Sumarização dos dados tratados
* Persistência dos dados sumarizado
  
### Setup do Projeto

Para o desenvolvimento do projetos utilizamos das seguintes tecnologias:
* Sistema Operacional Linux Ubuntu 22.04
* Docker 24.0.2
* Python 3.8.8
* Apache-Airflow 2.6.0


### Como rodar ?

Para executar o projeto é necessário um ambiente linux e instalar algumas das ferramentas listadas no tópico anterior. Para implementar esse projeto, foi utilizado Containers Docker, para melhor versatilidade em estações de trabalho. Por isso, recomendamos seguir o tutorial de instalação na documentação oficial do [Apache-Airflow](https://airflow.apache.org/docs/apache-airflow/2.6.0/howto/docker-compose/index.html)

As dependências do projeto, encontra-se no arquivo, deste repositório, requirements.txt

Os arquivos **csv** da base de dados, devem baixados diretamento do repositório dos dados aberto do INEP:
* [Censo Escolar 2015](https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2015.zip) 
* [Notas Enem por escola 2005 à 2015](https://download.inep.gov.br/microdados/enem_por_escola/2005_a_2015/microdados_enem_por_escola.zip)

Esses arquivos ficar na pasta **/dags/csv_files**

### Resultados