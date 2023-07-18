import json
import os
import csv
import re
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


# Step 1 of cleaning - remove "None"

def update_json_string_none_to_null(json_string: str) -> str:
    return json_string.replace("None", "null")

def reformat_json_quotes(json_string: str) -> str:
        pattern = r"(?<![A-Za-z])'(?![A-Za-z])|(?<=[A-Za-z])'(?![A-Za-z])|(?<![A-Za-z])'(?=[A-Za-z])"
        fixed_str = re.sub(pattern, '"', json_string)
        return fixed_str

