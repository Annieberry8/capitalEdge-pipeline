import re
import apache_beam as beam
from google.cloud import bigquery
import argparse
from apache_beam.options.pipeline_options import PipelineOptions

# Define argument
parser = argparse.ArgumentParser
parser.add_argument('..input', dest='input', required=True, help='Input file to process')
paths_args, Pipelines_args = parser.parse_known_args()

input_file = paths_args.input

stock_prices_table_spec = "capitaledge-pipeline:capitaledge_dataset.cleaned_stock_prices_data"

#initialize Bigquery
client = bigquery.Client()
dataset_id = "capitaledge-pipeline.capitaledge_dataset"

try:
    client.get_dataset(dataset_id)
except:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "london"
    dataset_description = "Dataset for stock prices"
    client.create_dataset(dataset_id, exists_ok=True)
    
    def to_json(row):
        fields = row.split(',')
        return{
            "Date":fields[0],
            "open":fields[1],
            "high":fields[2],
            "low":fields[3],
            "close":fields[4],
            "volume":fields[5],
            "new_column":fields[6],
            
        }
    
table_schema = '''
    Date:date,
    open:float,
    high:float,
    low:float,
    close:float,
    volume:int,
    new_column:int
'''

options = PipelineOptions(Pipelines_args)
    
with beam.Pipeline(options=options) as p:
    cleaned_stock_prices_data = (
        p
        | "Read Input File" >> beam.io.ReadFromText(input_file, skip_header_lines=1) 
        | "Add count column">> beam.io.Map(lambda row:row + "1")
    ) 
    
(cleaned_stock_prices_data
    | "delivered to JSON" >> beam.Map(to_json)
    | "Write Cleaned Stock Prices Data to Bigquery" >> beam.io.WriteToBigQuery(
        stock_prices_table_spec,
        schema = table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
    )
    
 ) 
