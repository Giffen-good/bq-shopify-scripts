from google.cloud import bigquery
from util import reformat_json_quotes, update_json_string_none_to_null, reformat_json_string
import json





# Set batch size
BATCH_SIZE = 400

# Set columns for query
if __name__ == "__main__":
    project_id: str = 'no-maintenance'
    client = bigquery.Client(project=project_id)
    query = f"""
   SELECT *
        FROM (
          SELECT
            IF(JSON_EXTRACT(shipping_address, '$') IS NULL, 'Invalid', 'Valid') AS is_valid_json, shipping_address, id
          FROM
            `no-maintenance`.shopify.orders
          WHERE
            shipping_address IS NOT NULL
        )
        WHERE
          is_valid_json = 'Invalid';
"""
    query_job = client.query(query)
    query_results = list(query_job.result())
    processed_rows_count = 0;
    # For loop to begin batch processing
    for n in range(0, len(query_results), BATCH_SIZE):
        batch = query_results[n: n + BATCH_SIZE]
        when_then_clauses= []
        # For row in batch, all rows should be cleaned! Check the results

        ids = []
        for row in batch:
            id = row.id
            shipping_address = row.shipping_address
            reformatted_shipping_address = reformat_json_string(shipping_address)
            if reformatted_shipping_address is None:
                continue
            clause_string = f"WHEN id = {id} THEN '{reformatted_shipping_address}'"
            when_then_clauses.append(clause_string)
            ids.append(id)
        update_query = f"""
                    UPDATE `no-maintenance`.shopify.orders
                    SET shipping_address = CASE
                        {" ".join(when_then_clauses)}
                    END
                    WHERE id IN ({",".join(str(id) for id in ids)});
                """
        query_job = client.query(update_query)
        query_job.result()
        processed_rows_count += len(ids)
        print(f"Updated {processed_rows_count}/{len(query_results)} rows")