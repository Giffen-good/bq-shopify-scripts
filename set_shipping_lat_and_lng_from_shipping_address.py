from google.cloud import bigquery
import json
from util import reformat_json_string
from get_geolocation_data import set_geolocation_on_orders

BATCH_SIZE = 300

if __name__ == "__main__":
    client = bigquery.Client()
    query = """
    SELECT shipping_address, id
    FROM `no-maintenance`.shopify.orders 
    WHERE shipping_latitude IS NULL
    AND shipping_address IS NOT NULL
    """
    query_job = client.query(query)
    query_results = list(query_job.result())

    n = 1
    for i in range(0, len(query_results), BATCH_SIZE):
        batch = query_results[i:i + BATCH_SIZE]
        batch_tuples = []

        for row in batch:

            formatted_shipping_address = reformat_json_string(row["shipping_address"])

            shipping_address = json.loads(formatted_shipping_address)

            if shipping_address["latitude"] is not None and shipping_address["longitude"] is not None:
                geo_tuple = (row["id"], shipping_address["latitude"], shipping_address["longitude"])
                batch_tuples.append(geo_tuple)

        set_geolocation_on_orders(client, batch_tuples)
        print(f'updated {BATCH_SIZE*n} of {len(query_results)}')
        n += 1













