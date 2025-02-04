from datetime import datetime, timedelta

from ecommerce import Products, Users, Orders

import random

LABEL_PREFIX = 'LCR'
NUM_ITEMS = random.randint(1, 5)
PRICING = [18.5,19.0,20.0,21.0]

NUM_USERS = random.randint(25, 50)
LOCALES = ['en_GB','en_US','fr_FR','en_CA','de_DE']

NUM_ORDERS = random.randint(75, 200)
MAX_ITEMS_PER_ORDER = 7
# ORDERS_START_DATE = datetime.strptime('2025-01-18','%Y-%m-%d')
MESSY_DATA = False


db_path = 'database/ecommerce_data.db'

def main():

    orders_start_date = (datetime.today() - timedelta(days = 2000))
    orders_end_date = orders_start_date + timedelta(days=7)
    products = Products(db_path)
    users = Users(db_path)
    orders = Orders(db_path)
    products._drop_db_table()
    users._drop_db_table()
    orders._drop_db_table()

    for i in range(10):
        users.create(NUM_USERS, LOCALES)
        products.create(LABEL_PREFIX,NUM_ITEMS,PRICING)
        orders.create(users,products,NUM_ORDERS,MAX_ITEMS_PER_ORDER,orders_start_date, orders_end_date.strftime('%Y-%m-%d'))
        orders_start_date += timedelta(days=7)
        orders_end_date += timedelta(days=7)

    users.to_csv()
    orders.to_csv()

if __name__ == '__main__':
    main()
    # try:
    #     main()
    # except Exception as e:
    #     print(e)

        # sqlite3.OperationalError