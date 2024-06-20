from airflow.decorators import dag,task
import requests
import os
import json
from airflow.models import Variable
from google.cloud import pubsub_v1

@dag("stock_market_data", schedule=None, tags=["fetch", "stock_market_data"])
def stock_market_data():
    @task()
    def fetch_api_key():
        api_key = Variable.get("api_key")
        return api_key

    @task()
    def fetch_stock_market_data(api_key=None):
        url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MSFT&apikey={api_key}"
        response = requests.get(url)
        return response.json()
    
    @task()
    def publish_data_to_gcp(stock_market_data=None):
        gcp_auth_credentials = {
            "type": "service_account",
            "project_id": Variable.get("project_id"),
            "private_key_id": Variable.get("private_key_id"),
            "private_key": Variable.get("private_key"),
            "client_email": Variable.get("client_email"),
            "client_id": Variable.get("client_id"),
            "auth_uri": Variable.get("auth_uri"),
            "token_uri": Variable.get("token_uri"),
            "auth_provider_x509_cert_url": Variable.get("auth_provider_x509_cert_url"),
            "client_x509_cert_url": Variable.get("client_x509_cert_url")
        }
        publisher = pubsub_v1.PublisherClient.from_service_account_info(gcp_auth_credentials)
        topic_path = publisher.topic_path(Variable.get("project_id"), Variable.get("topic_id"))
        current_date = stock_market_data["Meta Data"]["3. Last Refreshed"]
        message = 'stock-market-{}'.format(current_date)
        attributes = stock_market_data['Time Series (Daily)'][current_date]
        attributes['date'] = current_date
        publish_future = publisher.publish(topic_path, message.encode("utf-8"),**attributes)
        print(publish_future.result())

    api_key = fetch_api_key()
    stock_market_data = fetch_stock_market_data(api_key)
    publish_data_to_gcp(stock_market_data)

stock_market_data()