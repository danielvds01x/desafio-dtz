import apache_beam as beam
import logging
import pandas as pd
import json
import gcsfs
import csv

from datetime import datetime
from google.cloud import storage
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions, SetupOptions, PipelineOptions

##Revisar
#Funcao dataprocesso obtem a datahora para incluir na base que vai para o bq
# def dataprocesso(element):
    # from datetime import datetime
    # #hora do processo, sera incluido no campo process_date
    # dateprocess = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    # logging.info ( "Pipeline arguments: {}".format ( element + [dateprocess] ) )
    # return element

# =============================================================================
# Classe ExtrairLinhas faz a limpeza e formata para gravacao na Tabela
# =============================================================================
class ExtrairLinhas ( beam.DoFn ):
    def process(self, element):
        #Funcao para transformar uma lista aninhada em uma lista flat ( usa recursao, cool:) )
        def flatten(d, parent_key='', sep='_'):
            items = []
            for k, v in d.items ():
                new_key = parent_key + sep + k if parent_key else k
                if isinstance ( v, collections.MutableMapping ):
                    items.extend ( flatten ( v, new_key, sep = sep ).items () )
                else:
                    items.append ( (new_key, v) )
            return dict ( items )   
    
        # cria um dicionario
        data = {}

        # Define tube_assembly_id como key
        # e adiciona as linhas no dicionario
        for rows in element:
            key = rows [ 'tube_assembly_id' ]
            data [ key ] = rows

        # dicionario formato flat
        flat_data = flatten ( data )

        # Removendo as linhas com NA e os TA-
        for key, value in dict(flat_data).items():
            if value == 'NA' or value.find ( 'TA-' ) != -1:
                del flat_data [ key ]

        # tmp_field_tube_assembly_id = ''
        # tmp_field_component_id = ''
        # tmp_field_quantity = 0
        temp = list ( flat_data )
        
        #Revisando essa iteracao, ainda estou testando
        #Preciso entender como fazer para incluir os items formatados em uma nova 
        # estrutura (dict, list, etc) pra exportar
        for key, value in flat_data.items ():
            """ os dados sao extraidos usando 2 linhas por iteracao """
            if key.find ( 'component_id' ) != -1:
                # print(key[0:8]) #tube_assembly_id
                tmp_field_tube_assembly_id = str ( key [ 0:8 ] )
                # print(value)  #component_id
                tmp_field_component_id = str ( value )
                # print(dic[temp[temp.index(key) + 1]]) #quantity
                tmp_field_quantity = int ( flat_data [ temp [ temp.index ( key ) + 1 ] ] )
        
        return flat_data
        # DateTime para o campo date_process
        # datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        # timestamp_from_bigquery.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

# =============================================================================
# Construcao e Execucao do pipeline
# =============================================================================
def run(argv=None):
    pipeline_options = PipelineOptions( flags=argv,
        runner='DataflowRunner',
        project='desafio-dotz',
        job_name='load-csv-to-bq-tb-bill-of-materials',
        staging_location='gs://desafio-dotz/staging',
        region='us-central1'
        )
    logging.info ( "Pipeline arguments: {}".format ( pipeline_options ) )

    # obtendo o schema da tabela no bucket
    gcs_file_system = gcsfs.GCSFileSystem(project="desafio-dotz")
    gcs_json_path = 'gs://desafio-dotz/table_schemas/schema_tb_bill_of_materials.json'
    with gcs_file_system.open(gcs_json_path) as json_file:
        table_schema = json.load ( json_file )
    table_spec = bigquery.TableReference (
        projectId = 'desafio-dotz',
        datasetId = 'raw',
        tableId = 'tb_bill_of_materials' )
    p = beam.Pipeline ( options = pipeline_options )
    read = (p
            | 'Leitura Arquivo CSV' >> beam.io.ReadFromText (
                'gs://csv-repo-5gj7rn4mk4bosrcpg5ew4lqlj0ulf1dy/bill_of_materials.csv' )
            | 'Extrair Linhas' >> beam.ParDo ( ExtrairLinhas () )
            # | 'Campo process_date' >> beam.Map(dataprocesso)
            )
    bq = (
            read
            | "Escrever no BQ" >> beam.io.WriteToBigQuery (
        table_spec,
        schema = table_schema,
        write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )
    )
    result = p.run ()
    result.wait_until_finish ()  # For it to hold the terminal until it finishes
run()



