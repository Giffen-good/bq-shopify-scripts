from dotenv import load_dotenv
import pandas as pd
from util import fetch_paginated_data

load_dotenv()

if __name__ == '__main__':
    params = {
        'status': 'any',
        'limit': 250
    }
    fetch_paginated_data('orders', 'orders', params)
