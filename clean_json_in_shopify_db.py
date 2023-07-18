from google.cloud import bigquery
from util import reformat_json_quotes, update_json_string_none_to_null, reformat_json_string
import json





# Set batch size
BATCH_SIZE = 200

# Set columns for query
if __name__ == "__main__":
    project_id: str = 'no-maintenance'
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT id, shipping_address 
    FROM `no-maintenance`.shopify.orders
    WHERE shipping_address IS NOT NULL
    AND JSON_EXTRACT_SCALAR(shipping_address, '$') IS NULL;
"""
    query_job = client.query(query)
    query_results = list(query_job.result())

    # For loop to begin batch processing
    for n in range(0, len(query_results), BATCH_SIZE):
        batch = query_results[n: n + BATCH_SIZE]
        when_then_clauses= []
        # For row in batch, all rows should be cleaned! Check the results

        ids = []
        for row in batch:
            id = row.id
            shipping_address = row.shipping_address
#            json_double_quotes = reformat_json_quotes(shipping_address)
            reformatted_shipping_address = reformat_json_string(shipping_address)
            if reformatted_shipping_address is None:
                continue
            clause_string = f"WHEN id = {id} THEN shipping_address = {reformatted_shipping_address}"
            when_then_clauses.append(clause_string)
            ids.append(id)
        update_query = f"""
                    UPDATE `no-maintenance`.shopify.orders
                    SET shipping_address = CASE {" ".join(when_then_clauses)} END
                    WHERE id IN {",".join(str(id) for id in ids)};
                """


    # query_job = client.query(update_query)
    # query_job.result()

        #
        # update_query = f"""
        # UPDATE `no-maintenance`.shopify.orders
        # SET shipping_address = {updated_shipping_address}
        # WHERE JSON_EXTRACT(shipping_address, '$') IS NULL
        # """

        #
        #
        # def get_affected_rows(client):
        #
        #     affected_rows = []
        #     schema = result.schema
        #     for row in result:
        #         row_values = []
        #         for field in schema
        #             row_values.append(row[field.name])
        #         affected_rows.append(row_values)
        #     return affected_rows
        #
        # affected_rows = get_affected_rows(client)
        #
        # for row in affected_rows:
        #     print("Affected Row:", row)
        #



        # # SET shipping_address = {updated_shipping_address} WHERE id = {id}"
        # # for id, updated_shipping_address in cleaned_rows:
        # #
        # #     query = f"CASE WHEN (JSON_EXTRACT(shipping_address, '$') IS FALSE
        # #             "CASE WHEN JSON_EXTRACT_SCALAR(shipping_address, '$') IS FALSE " \
        # #             "UPDATE `no-maintenance`.shopify.orders SET " \
        # #             "shipping_address = '{updated_shipping_address}' WHERE id = {id}"
        # #     query_job = client.query(query)
        # #     query_job.result()
        # #     print(str(query_job.result()))
