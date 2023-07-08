import os
from dotenv import load_dotenv
import requests
import pandas as pd

load_dotenv()

def get_next_page_link(response):
    link = response.headers['Link']
    next_page = link.split(';')[0].strip()[1:-1]
    return next_page

def has_next_page(response):
    link = response.headers['Link']
    return 'next' in link
def get_orders(shop_url: str, headers: dict) -> list:
    params = {
        'status': 'any',
        'limit': 250
    }
    all_orders = []
    # Send a GET request to the Shopify API
    response = requests.get(f"{shop_url}/admin/api/2023-07/orders.json", headers=headers, params=params)
    orders = response.json()['orders']
    all_orders.append(orders)
    while has_next_page(response):
        next_page = get_next_page_link(response)
        response = requests.get(next_page, headers=headers)
        orders = response.json()['orders']
        all_orders.append(orders)
    return all_orders

if __name__ == '__main__':
    # %%
    token = os.getenv('SHOPIFY_ACCESS_TOKEN')
    version = os.getenv('SHOPIFY_API_VERSION')
    shop_url = os.getenv('SHOPIFY_SHOP_URL')
    key = os.getenv('SHOPIFY_API_KEY')
    secret = os.getenv('SHOPIFY_API_SECRET')
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
    }
    orders = get_orders(shop_url, headers)
    df = pd.DataFrame(orders)
    df.to_csv('orders.csv', index=False)
