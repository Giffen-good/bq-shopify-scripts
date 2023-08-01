from google.cloud import bigquery

from util import get_last_published_row_id, update_shop_table_since_id, fetch_shopify_data_since_id


def update_shopify_orders(client):
    table = "`no-maintenance`.shopify.orders"
    since_id = get_last_published_row_id(client, table)
    update_shop_table_since_id(client, 'orders', since_id, "orders")

    # first page

    # update_shop_table_since_id(client, 'orders', since_id, "orders")


if __name__ == "__main__":
    client = bigquery.Client(project='no-maintenance')
    update_shopify_orders(client)

# 5381186355438
