from datetime import datetime, timedelta

from ecommerce import Products, Users, Orders

import random

LABEL_PREFIX = 'LCR'
PRICING = [18.5,19.0,20.0,21.0]

LOCALES = ['en_GB', 'en_US', 'fr_FR',
           'en_CA', 'de_DE', 'en_AU', 
           'es_ES', 'fr_BE', 'it_IT', 
           'ja_JP', 'nl_NL', 'pt_PT',]

MAX_ITEMS_PER_ORDER = 7
MESSY_DATA = False

DB_PATH = 'database/ecommerce_data.db'

def main():

    orders_start_date = (datetime.today() - timedelta(days = 2100))
    orders_end_date = orders_start_date + timedelta(days=7)
    products = Products(DB_PATH)
    users = Users(DB_PATH)
    orders = Orders(DB_PATH)
    # products._drop_db_table()
    # users._drop_db_table()
    # orders._drop_db_table()

    for i in range(50):

        num_items = random.randint(0, 3)
        num_users = random.randint(25, 50)
        num_orders = random.randint(75, 200)

        users.create(
            num_users=num_users,
             locales=LOCALES
             )
        
        products.create(
            label_prefix=LABEL_PREFIX,
            num_items=num_items,
            pricing=PRICING
            )
        
        orders.create(
            users=users,
            products=products,
            num_orders=num_orders,
            max_num_items=MAX_ITEMS_PER_ORDER,
            start_date=orders_start_date, 
            end_date=orders_end_date.strftime('%Y-%m-%d')
            )
        
        # orders.to_csv(start_date=datetime.strftime(orders_start_date,'%Y-%m-%d'))

        orders_start_date += timedelta(days=7)
        orders_end_date += timedelta(days=7)

    users.to_csv()
    orders.to_csv()


if __name__ == '__main__':

    try:
        main()
    except Exception as e:
        print(e)
