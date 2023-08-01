from google.cloud import bigquery

if __name__ == "__main__":
    client = bigquery.Client(project="no-maintenance")

    # Your SQL query
    query = """
    SELECT c.email AS Email, c.first_name AS `First Name`, c.last_name AS `Last Name`,
        JSON_EXTRACT(o.shipping_address, "$.country") AS Country,
        JSON_EXTRACT(o.shipping_address, "$.zip") AS Zip,
        c.phone AS Phone
    FROM `no-maintenance`.shopify.nm_customers_2023_07_18 AS c
    JOIN `no-maintenance`.shopify.orders AS o ON c.last_order_id = o.id
    WHERE o.created_at > '2023-05-18'
        AND (c.phone IS NOT NULL OR c.email IS NOT NULL)
    """

    # Configure the query job
    job_config = bigquery.QueryJobConfig()
    job_config.allow_large_results = True
    job_config.destination = "no-maintenance.shopify.g_ads_2023_07_18"
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # Start the query job
    query_job = client.query(query, job_config=job_config)

    # Wait for the query job to complete
    query_job.result()

    print("Query job completed.")

    # Export the results to a local CSV file
    filename = "output.csv"
    with open(filename, "w") as file:
        query_job.export_to_file(file, format_=bigquery.DestinationFormat.CSV)

    print(f"Results exported to: {filename}")
