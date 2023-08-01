import json
import os
import csv
import re
import requests
from dotenv import load_dotenv
import ast

from google.cloud import bigquery

load_dotenv()


def get_next_page_link(response):
    # #...
    # Link: "<https://{shop}.myshopify.com/admin/api/{api_version}/products.json?page_info=abcdefg&limit=3>; rel=previous, <https://{shop}.myshopify.com/admin/api/{api_version}/products.json?page_info=opqrstu&limit=3>; rel=next"
    # #...
    link = response.headers['Link']
    if has_previous_page(response):
        return link.split(',')[1].split(';')[0].strip()[1:-1]
    else:
        return link.split(';')[0].strip()[1:-1]


def has_previous_page(response):
    link = response.headers['Link']
    return 'previous' in link


def has_next_page(response):
    link = response.headers['Link']
    return 'next' in link


def write_to_csv(data, filename, field_names, write_header=False, mode='a'):
    with open(f'data/{filename}.csv', mode, newline='') as f:
        writer = csv.DictWriter(f, fieldnames=field_names)

        # Write the header row
        if write_header:
            writer.writeheader()

        # Write each dictionary as a separate row in the CSV file
        writer.writerows(data)


def fetch_shopify_data_since_id(endpoint: str, since_id: int, params={}):
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": os.getenv('SHOPIFY_ACCESS_TOKEN'),
    }
    params["since_id"] = since_id
    response = requests.get(
        f"{os.getenv('SHOPIFY_SHOP_URL')}/admin/api/{os.getenv('SHOPIFY_API_VERSION')}/{endpoint}.json",
        headers=headers, params=params)

    res = response.json()[endpoint]
    if len(res) == 0:
        print('No data to write')
        return
    i = 0
    while has_next_page(response):
        i += 1
        next_page = get_next_page_link(response)
        print(f"page #{i} - {next_page}")
        response = requests.get(next_page, headers=headers)
        data = response.json()[endpoint]
        res = [*res, *data]
    return res


def update_shop_table_since_id(client, endpoint: str, since_id: int, bq_table_id: str, params={}):
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": os.getenv('SHOPIFY_ACCESS_TOKEN'),
    }
    params["since_id"] = since_id
    response = requests.get(
        f"{os.getenv('SHOPIFY_SHOP_URL')}/admin/api/{os.getenv('SHOPIFY_API_VERSION')}/{endpoint}.json",
        headers=headers, params=params)
    data = response.json()[endpoint]
    field_names = data[0].keys()
    if len(data) == 0:
        print('No data to write')
        return
    write_shop_data_to_bq(client, bq_table_id, data)
    i = 0
    while has_next_page(response):
        i += 1
        next_page = get_next_page_link(response)
        print(f"page #{i} - {next_page}")
        response = requests.get(next_page, headers=headers)
        data = response.json()[endpoint]
        write_shop_data_to_bq(client, bq_table_id, data)


def write_shop_data_to_bq(client, table_id: str, data: list[dict]):
    # Get table reference
    table_ref = client.dataset(os.getenv("BIGQUERY_SHOP_DATASET_ID")).table(table_id)
    # Get table object
    table = client.get_table(table_ref)
    json_str = json.dumps(data)
    # Assuming the response is in JSON format, this will be a JSON string
    print(json_str)
    # Then you can load it into BigQuery as JSON
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_file(json_str, table, job_config=job_config)
    errors = job.result()

    if errors:
        print('Errors:')
        print(errors)
    else:
        print('Rows inserted successfully.')


def write_local_shopify_csv(endpoint: str, filename: str, params=None, append_to_existing_file: bool = True):
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": os.getenv('SHOPIFY_ACCESS_TOKEN'),
    }
    response = requests.get(
        f"{os.getenv('SHOPIFY_SHOP_URL')}/admin/api/{os.getenv('SHOPIFY_API_VERSION')}/{endpoint}.json",
        headers=headers, params=params)
    data = response.json()[endpoint]
    field_names = data[0].keys()
    if len(data) == 0:
        print('No data to write')
        return
    if append_to_existing_file:
        write_to_csv(data, filename, field_names, True, 'a')
    else:
        write_to_csv(data, filename, field_names, True, 'w')
    i = 0
    while has_next_page(response):
        i += 1
        next_page = get_next_page_link(response)
        print(f"page #{i} - {next_page}")
        response = requests.get(next_page, headers=headers)
        write_to_csv(response.json()[endpoint], filename, field_names)


def reformat_json_string(json_string: str) -> str:
    # Replace Python None, True, False with JSON null, true, false
    json_string = json_string.replace('None', 'null')
    json_string = json_string.replace('True', 'true')
    json_string = json_string.replace('False', 'false')

    # Replace single quotes with double quotes, but avoid replacing apostrophes within words
    json_string = re.sub(r"(?<=\W)'|'(?=\W)|^'|'$", '"', json_string)
    json_string = json_string.replace("'", "\\'")
    try:
        # Attempt to load the string as JSON to ensure it's valid
        json_dict = json.loads(json_string)
        # Dump the JSON back to a string, this will ensure it has the correct formatting
        json_string_proper = json.dumps(json_dict)
        return json_string_proper

    except json.JSONDecodeError:
        print(f"Couldn't parse the provided string into JSON =>\n\t{json_string}")
        return None


def get_last_published_row_id(client, table):
    query = f"""
       SELECT id
            FROM {table}
            ORDER BY ID DESC LIMIT 1
    """
    query_job = client.query(query)
    res = list(query_job.result())
    return res[0]["id"]


def update_json_string_none_to_null(json_string: str) -> str:
    return json_string.replace("None", "null")


def reformat_json_quotes(json_string: str) -> str:
    pattern = r"(?<![A-Za-z])'(?![A-Za-z])|(?<=[A-Za-z])'(?![A-Za-z])|(?<![A-Za-z])'(?=[A-Za-z])"
    fixed_str = re.sub(pattern, '"', json_string)
    return fixed_str
