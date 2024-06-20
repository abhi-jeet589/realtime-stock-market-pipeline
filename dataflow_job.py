import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import json,os
from apache_beam.options.pipeline_options import PipelineOptions

credentials_path = './config/credentials/service-account-private-key.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

options = PipelineOptions(
    runner = 'DirectRunner',
    streaming=True
)

project_id = "psyched-zone-422610-j4"
subscription_name = "stock-market-topic-sub"

stock_market_table_schema = {
    'fields': [
        {'name': 'open', 'type': 'FLOAT'},
        {'name': 'high', 'type': 'FLOAT'},
        {'name': 'low', 'type': 'FLOAT'},
        {'name': 'close', 'type': 'FLOAT'},
        {'name': 'volume', 'type': 'FLOAT'},
        {'name': 'date', 'type': 'DATE'}
    ]
}

with beam.Pipeline(options=options) as pipeline:
    _ = (
        pipeline
        | 'ReadFromPubSub' >> ReadFromPubSub(
            subscription=f'projects/{project_id}/subscriptions/{subscription_name}',with_attributes=True
        )
        | "Pick attributes" >> beam.Map(lambda x: x.attributes)
        | "Change attribute fields" >> beam.Map(lambda data:
            {
                'open': float(data.get('1. open', None)),
                'close': float(data.get('4. close', None)),
                'high': float(data.get('2. high', None)),
                'low': float(data.get('3. low', None)),
                'volume': int(data.get('5. volume', None)),
                'date' : data.get('date', None)
            })
        | "Log Data" >> beam.Map(print)
        | "Write Stock Data to BigQuery" >> beam.io.WriteToBigQuery(
            schema=stock_market_table_schema,
            table = f'{project_id}:stock_market_dataset.stock_market_table',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
     )
     )