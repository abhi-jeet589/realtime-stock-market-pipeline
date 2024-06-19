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

with beam.Pipeline(options=options) as pipeline:
    _ = (
        pipeline
        | 'ReadFromPubSub' >> ReadFromPubSub(
            subscription=f'projects/{project_id}/subscriptions/{subscription_name}'
        )
        | beam.Map(print)
     )