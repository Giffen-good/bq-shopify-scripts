from dotenv import load_dotenv
import pandas as pd
from util import fetch_shop_data_since_id

load_dotenv()

if __name__ == '__main__':
    params = {
        'limit': 250
    }

    # fetch_paginated_data('customers', 'customers', params)
