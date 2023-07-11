import json
import os
import csv
import requests
from dotenv import load_dotenv
import ast

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


def fetch_paginated_data(endpoint: str, filename: str, params=None, append_to_existing_file: bool = True):
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


def update_json_string_from_none_to_null(json_string: str) -> str:
    return json_string.replace("None", "null")


def reformat_json_string(json_string:str) -> str:
    json_dict = ast.literal_eval(json_string)
    json_string_proper = json.dumps(json_dict)

    return json_string_proper



def update_json_string_none_to_none(json_string: str) -> str:
    return json_string.replace("None", "\"None\"")




#def clean_json_fields_by_column(client, table, column_name):
    # 1. SELECT column_name and id from all rows in table containing INVALID JSON string
    # 2. cleanup the JSON string
    # 3. Write a batch UPDATE using CASE/WHEN to update each row in (1) with the clean JSON in (2)
