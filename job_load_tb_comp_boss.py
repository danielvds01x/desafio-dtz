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

# =============================================================================
# Classe ExtrairLinhas faz a extracao do arquivo para gravacao no bq
# =============================================================================

# Funcao parse_file que faz o parse do csv e coloca em format de list
def parse_file(element):
    import csv
    for line in csv.reader([element], quotechar=None, delimiter=',', quoting=csv.QUOTE_NONE, skipinitialspace=False):
        print(line)
        return line

#Inclui a data do processo na linha para subir no bq
def dataprocesso(element):
    from datetime import datetime
    #hora do processo, sera incluido no campo process_date
    dateprocess = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    logging.info ( "Pipeline arguments: {}".format ( element + [dateprocess] ) )
    row = element.append(dateprocess)
    return row

# Funcao para fazer a conversao de tipos dos elementos antes de entrar no bq
# def casting(element):
    # Schema da tabela com os respectivos tipos
    # 'component_id' : component_id_,
    # 'component_type_id' : component_type_id_,
    # 'type' : type_,
    # 'connection_type_id' : connection_type_id_,
    # 'outside_shape' : outside_shape_,
    # 'base_type' : base_type_,
    # 'height_over_tube' : float(height_over_tube_),
    # 'bolt_pattern_long' : float(bolt_pattern_long_),
    # 'bolt_pattern_wide' : float(bolt_pattern_wide_),
    # 'groove' : groove_,
    # 'base_diameter' : float(base_diameter_),
    # 'shoulder_diameter' : float(shoulder_diameter_),
    # 'unique_feature' : unique_feature_,
    # 'orientation' : orientation_,
    # 'weight' : float(weight_)
        
    # Fazendo cast dos campos na sequencia
    # types = (str, str, str, str, str, str, float, float, float, str, float, float, str, str, float)
    # line = (typ(value) for typ, value in zip(types, element))
    # return line

# class ExtrairLinhas( beam.DoFn ):    
    # def process( self, element ):
        # component_id_, component_type_id_, type_, connection_type_id_,
        # outside_shape_, base_type_, height_over_tube_, bolt_pattern_long_,
        # bolt_pattern_wide_, groove_, base_diameter_, shoulder_diameter_,
        # unique_feature_, orientation_, weight = element.split(',')
        # return [
            # {
                # 'component_id' : component_id_,
                # 'component_type_id' : component_type_id_,
                # 'type' : type_,
                # 'connection_type_id' : connection_type_id_,
                # 'outside_shape' : outside_shape_,
                # 'base_type' : base_type_,
                # 'height_over_tube' : float(height_over_tube_),
                # 'bolt_pattern_long' : float(bolt_pattern_long_),
                # 'bolt_pattern_wide' : float(bolt_pattern_wide_),
                # 'groove' : groove_,
                # 'base_diameter' : float(base_diameter_),
                # 'shoulder_diameter' : float(shoulder_diameter_),
                # 'unique_feature' : unique_feature_,
                # 'orientation' : orientation_,
                # 'weight' : float(weight_)
            # }
        # ]

# =============================================================================
# Build and run the pipeline
# =============================================================================

def run(argv=None):
    pipeline_options = PipelineOptions( flags=argv,
        runner='DataflowRunner',
        project='desafio-dotz',
        job_name='load-csv-to-bq-comp-boss',
        staging_location='gs://desafio-dotz/staging',
        region='us-central1'
        )
    logging.info ( "Pipeline arguments: {}".format ( pipeline_options ) )

    # obtendo o schema da tabela no bucket
    gcs_file_system = gcsfs.GCSFileSystem(project="desafio-dotz")
    gcs_json_path = 'gs://desafio-dotz/table_schemas/schema_tb_comp_boss.json'
    with gcs_file_system.open(gcs_json_path) as json_file:
        table_schema = json.load ( json_file )
    table_spec = bigquery.TableReference (
        projectId = 'desafio-dotz',
        datasetId = 'raw',
        tableId = 'tb_comp_boss' )
    p = beam.Pipeline ( options = pipeline_options )
    read = (p
            | 'Leitura Arquivo CSV' >> beam.io.ReadFromText (
                'gs://csv-repo-5gj7rn4mk4bosrcpg5ew4lqlj0ulf1dy/comp_boss.csv' )
            # | 'Extrair Linhas' >> beam.ParDo ( ExtrairLinhas () )
            | 'Parse Arquivo CSV' >> beam.Map(parse_file)
            | 'Debug 1' >> beam.Map(print)
            # | 'Casting dos Campos' >> beam.Map(casting)
            | 'Campo process_date' >> beam.Map(dataprocesso)
            | 'Debug 2' >> beam.Map(print)
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

