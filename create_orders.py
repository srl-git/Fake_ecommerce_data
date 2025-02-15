from ecommerce import Ecommerce
import random
from datetime import datetime

DB_PATH = 'database/ecommerce_data.db'

LOCALES = ['en_GB', 'en_US', 'fr_FR',
           'en_CA', 'de_DE', 'en_AU', 
           'es_ES', 'fr_BE', 'it_IT', 
           'ja_JP', 'nl_NL', 'pt_PT']

MAX_ITEMS_PER_ORDER = 7

MESSY_DATA = False


def main():

    ecommerce = Ecommerce(DB_PATH)

    num_orders = random.randint(75, 500)
        
    ecommerce.create_orders_and_users(
        locales=LOCALES,
        num_orders=num_orders,
        max_num_items=MAX_ITEMS_PER_ORDER
    )

    ecommerce.to_csv(
        start_date=datetime.now(),
        local_file=False,
        cloud_storage_file=True
    )

if __name__ == '__main__':

    main()
