from dotenv import load_dotenv
from util import fetch_shopify_data_since_id, insert_json_rows_to_bq
from google.cloud import bigquery as bq
load_dotenv()

if __name__ == '__main__':
    params = {
        'status': 'any',
        'limit': 250
    }
    client = bq.Client(project="no-maintenance")
    data = fetch_shopify_data_since_id(client, 'orders', None, params)

