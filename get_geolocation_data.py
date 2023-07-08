import os
import requests
from google.cloud import bigquery
from urllib.parse import quote
from dotenv import load_dotenv
import concurrent.futures
import threading
import time

load_dotenv()

RATE_LIMIT = 50
BATCH_SIZE = 300
semaphore = threading.Semaphore(RATE_LIMIT)


def get_lat_lng(address: str):
    lat, lng = None, None
    api_key = os.getenv('GOOGLE_GEOLOCATION_API_KEY')
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    endpoint = f"{base_url}?address={address}&key={api_key}"
    r = requests.get(endpoint)
    if r.status_code not in range(200, 299):
        print("Error occurred!")
        print(r.status_code)
    try:
        results = r.json()['results'][0]
        lat = results['geometry']['location']['lat']
        lng = results['geometry']['location']['lng']
    except:
        pass
    return lat, lng


def get_encoded_address(r) -> str:
    address_parts = [r['address1'], r['city'], r['province'], r['zip'], r['country']]
    a = ' '.join(part for part in address_parts if part is not None)
    return quote(a, safe='')

def get_lat_lng_throttled(address):
    with semaphore:
        return get_lat_lng(address)

def set_geolocation_on_orders(client, updates: list):
    # Construct SQL CASE statement
    lat_cases = " ".join(f"WHEN id={id} THEN {lat}" for id, lat, _ in updates)
    lng_cases = " ".join(f"WHEN id={id} THEN {lng}" for id, _, lng in updates)
    ids = ",".join(str(id) for id, _, _ in updates)

    query = f"""
        UPDATE `no-maintenance`.shopify.orders
        SET shipping_latitude = CASE {lat_cases} END,
            shipping_longitude = CASE {lng_cases} END
        WHERE id IN ({ids});
    """
    query_job = client.query(query)
    query_job.result()


if __name__ == "__main__":
    client = bigquery.Client()
    query = """
       SELECT JSON_EXTRACT_SCALAR(shipping_address, '$.address1') AS address1,
          JSON_EXTRACT_SCALAR(shipping_address, '$.city') AS city,
          JSON_EXTRACT_SCALAR(shipping_address, '$.province') AS province,
          JSON_EXTRACT_SCALAR(shipping_address, '$.zip') AS zip,
          JSON_EXTRACT_SCALAR(shipping_address, '$.country') AS country,
          ARRAY_AGG(id) AS order_ids
        FROM `no-maintenance`.shopify.orders
        WHERE shipping_address IS NOT NULL AND shipping_latitude IS NULL AND shipping_longitude IS NULL
            AND JSON_EXTRACT_SCALAR(shipping_address, '$.address1') IS NOT NULL
            AND JSON_EXTRACT_SCALAR(shipping_address, '$.city') IS NOT NULL
            AND JSON_EXTRACT_SCALAR(shipping_address, '$.address1') != ''
            AND JSON_EXTRACT_SCALAR(shipping_address, '$.city') != ''
        GROUP BY address1, city, province, zip, country;
       """
    query_job = client.query(query)
    query_results = query_job.result()
    result_count = query_results.total_rows
    query_results_list = list(query_results)

    update_count = 0
    for i in range(0, result_count, BATCH_SIZE):
        batch = query_results_list[i:i + BATCH_SIZE]
        addresses = [get_encoded_address(row) for row in batch]
        updates = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=RATE_LIMIT) as executor:
            lat_lngs = list(executor.map(get_lat_lng_throttled, addresses))

        for idx, (lat, lng) in enumerate(lat_lngs):
            for order_id in batch[idx]['order_ids']:
                updates.append((order_id, lat, lng))

        # Call the function with the updates list
        set_geolocation_on_orders(client, updates)
        update_count += len(updates)

        print(f"Updated {update_count} of {result_count}")
        # Clear updates for next batch
        updates.clear()

        # sleep to ensure we're not exceeding the Geocoding API rate limit (50 requests per second)
        # .... Is this necessary?
        time.sleep(1)




