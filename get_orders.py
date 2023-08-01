from dotenv import load_dotenv
import json
from util import fetch_shopify_data_since_id
import os
from google.cloud import bigquery as bq
load_dotenv()

def convert_nested_dict_to_json_string(data: dict):
    i = 0
    for key, value in data.items():
        i += 1
        if isinstance(value, dict):
            data[key] = json.dumps(value)
    print("columns : ", i)
    return data

def create_insert_statement(table_id: str, data: list[dict]):
    new_rows = []
    columns = []
    first = True
    for row in data[0:2]:
        print(row)
        if first:
            columns = row.keys()
            first = False
        vals = []
        for key, val in row.items():
            vals.append(val)

        new_row = f"({','.join(str(val) for val in vals)})"
        new_rows.append(new_row)

    insert_query = f"""
        INSERT INTO `no-maintenance`.shopify.{table_id} ({",".join(columns)})
        VALUES
            {",".join(new_rows)}
    """
    return insert_query

def add_data_to_bq_table(client, table_id: str, data: list[dict]):
    # Get table reference
    query = create_insert_statement(table_id, data)
    print(query)

    table_ref = client.dataset(os.getenv("BIGQUERY_SHOP_DATASET_ID")).table(table_id)
    # Get table object
    table = client.get_table(table_ref)
    job_config = bq.LoadJobConfig()
    job_config.source_format = bq.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True
    job_config.write_disposition = bq.WriteDisposition.WRITE_APPEND
    for row in data:
        convert_nested_dict_to_json_string(row)
    print(table.schema)
    sch = table.schema
    errors = client.insert_rows_json(table, data)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    # job = client.load_table_from_json(data, table_ref, job_config=job_config)  # Make an API request.
    # print("Loaded {} rows into {}:{}.".format(job.output_rows, table_id))

if __name__ == '__main__':
    params = {
        'status': 'any',
        'limit': 250
    }
    data = fetch_shopify_data_since_id('orders', '5381186355438', params)
    client = bq.Client(project="no-maintenance")
    add_data_to_bq_table(client, 'orders_bak', data)
    print(json)

