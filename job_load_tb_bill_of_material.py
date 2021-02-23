import apache_beam as beam
import logging
import pandas as pd
import json

from google.cloud import storage
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions, SetupOptions, PipelineOptions
import apache_beam as beam

# =============================================================================
# Build and run the pipeline
# =============================================================================
def run(argv=None):
    pipeline_options = PipelineOptions ( flags = argv )

    google_cloud_options = pipeline_options.view_as ( GoogleCloudOptions )
    google_cloud_options.project = 'desafio-dotz'  # change this
    google_cloud_options.job_name = 'load-csv-to-bq-bill-of-material'
    google_cloud_options.staging_location = 'gs://csv-repo-5gj7rn4mk4bosrcpg5ew4lqlj0ulf1dy/staging'  # change this
    google_cloud_options.temp_location = 'gs://csv-repo-5gj7rn4mk4bosrcpg5ew4lqlj0ulf1dy/temp'  # change this
    pipeline_options.view_as ( StandardOptions ).runner = 'DataflowRunner'
    pipeline_options.view_as ( SetupOptions ).save_main_session = True
    pipeline_options.view_as ( SetupOptions ).setup_file = "./setup.py"
    pipeline_options.view_as ( StandardOptions ).region = 'us-central1'
    logging.info ( "Pipeline arguments: {}".format ( pipeline_options ) )

    # obtendo o schema da tabela no bucket
    with open ( 'gs://desafio-dotz/table_schemas/schema_tb_bill_of_materials.json' ) as json_file:
        table_schema = json.load ( json_file )
    table_spec = bigquery.TableReference (
        projectId = 'desafio-dotz',
        datasetId = 'raw',
        tableId = 'tb_bill_of_materials' )
    p = beam.Pipeline ( options = pipeline_options )
    read = (p
            | 'Leitura Arquivo CSV' >> beam.io.ReadFromText (
                'gs://csv-repo-5gj7rn4mk4bosrcpg5ew4lqlj0ulf1dy/staging/bill_of_materials.csv' )
            | 'Extrair Linhas' >> beam.ParDo ( ExtrairLinhas () )
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


# =============================================================================
# Classe ExtrairLinhas faz a limpeza e formata para gravacao na Tabela
# =============================================================================
class ExtrairLinhas ( beam.DoFn ):
    def process(self, element):
        # cria um dicionario
        data = {}

        # Convert each row into a dictionary
        # and add it to data
        for rows in element:
            key = rows [ 'tube_assembly_id' ]
            data [ key ] = rows

            # printing final dictionary
            flat_data = flatten ( data )

            # Delete an item from dictionary while iteration
            for key, value in dict ( flat_data ).items ():
                if value == 'NA' or value.find ( 'TA-' ) != -1:
                    del flat_data [ key ]

        tmp_field_tube_assembly_id = ''
        tmp_field_component_id = ''
        tmp_field_quantity = 0

        temp = list ( flat_data )

        for key, value in flat_data.items ():
            # os dados sao extraidos usando 2 linhas por iteracao
            if key.find ( 'component_id' ) != -1:
                # print(key[0:8]) #tube_assembly_id
                tmp_field_tube_assembly_id = str ( key [ 0:8 ] )
                # print(value)  #component_id
                tmp_field_component_id = str ( value )
                # print(dic[temp[temp.index(key) + 1]]) #quantity
                tmp_field_quantity = int ( flat_data [ temp [ temp.index ( key ) + 1 ] ] )

        # DateTime para o campo date_process
        # datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        # timestamp_from_bigquery.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        def flatten(d, parent_key='', sep='_'):
            items = [ ]
            for k, v in d.items ():
                new_key = parent_key + sep + k if parent_key else k
                if isinstance ( v, collections.MutableMapping ):
                    items.extend ( flatten ( v, new_key, sep = sep ).items () )
                else:
                    items.append ( (new_key, v) )
            return dict ( items )
