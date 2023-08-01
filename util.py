import json
import os
import re
import requests
from dotenv import load_dotenv
from google.cloud import bigquery as bq

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

def fetch_shopify_data_since_id(client, endpoint: str, since_id: int, params={}):
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": os.getenv('SHOPIFY_ACCESS_TOKEN'),
    }
    if since_id is not None:
        params["since_id"] = since_id
    response = requests.get(
        f"{os.getenv('SHOPIFY_SHOP_URL')}/admin/api/{os.getenv('SHOPIFY_API_VERSION')}/{endpoint}.json",
        headers=headers, params=params)

    res = response.json()[endpoint]
    if len(res) == 0:
        print('No data to write')
        return
    i = 0
    insert_json_rows_to_bq(client, endpoint, res)
    while has_next_page(response):
        i += 1
        next_page = get_next_page_link(response)
        print(f"page #{i} - {next_page}")
        response = requests.get(next_page, headers=headers)
        data = response.json()[endpoint]
        insert_json_rows_to_bq(client, endpoint, data)
    return res

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


def convert_nested_dict_to_json_string(data: dict):
    for key, value in data.items():
        if isinstance(value, dict) or isinstance(value, list):
            data[key] = json.dumps(value)
    return data


def insert_json_rows_to_bq(client, table_id: str, data: list[dict]):
    table_ref = client.dataset(os.getenv("BIGQUERY_SHOP_DATASET_ID")).table(table_id)
    # Get table object
    table = client.get_table(table_ref)
    job_config = bq.LoadJobConfig()
    job_config.source_format = bq.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True
    job_config.write_disposition = bq.WriteDisposition.WRITE_APPEND
    for row in data:
        convert_nested_dict_to_json_string(row)
    errors = client.insert_rows_json(table, data)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    # job = client.load_table_from_json(data, table_ref, job_config=job_config)  # Make an API request.
    # print("Loaded {} rows into {}:{}.".format(job.output_rows, table_id))
