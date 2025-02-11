from datetime import datetime, timedelta
from ecommerce import Ecommerce, Products, Users, Orders
import random

DB_PATH = 'database/ecommerce_data.db'

LABEL_PREFIX = 'LCR'

PRICING = [18.0, 19.0,
           20.0, 21.0]

LOCALES = ['en_GB', 'en_US', 'fr_FR',
           'en_CA', 'de_DE', 'en_AU', 
           'es_ES', 'fr_BE', 'it_IT', 
           'ja_JP', 'nl_NL', 'pt_PT']

MAX_ITEMS_PER_ORDER = 7

MESSY_DATA = False


if __name__ == '__main__':

    ecommerce = Ecommerce(DB_PATH)

    print(ecommerce)

    ecommerce.users.create()

    # try:
    #     products = Products(DB_PATH)
    #     users = Users(DB_PATH)
    #     orders = Orders(DB_PATH)

    #     start_date = (datetime.today() - timedelta(days = 2100))
    #     end_date = start_date + timedelta(days=7)

    #     for i in range(1):

    #         # num_items = random.randint(1, 3)
    #         # num_users = random.randint(25, 100)
    #         # num_orders = round(num_users * random.uniform(1, 1.3))

    #         num_orders = 10

    #         # users.create(
    #         #     5,
    #         #     LOCALES,
    #         #     start_date
    #         # )
            
    #         # products.create(
    #         #     LABEL_PREFIX,
    #         #     num_items,
    #         #     PRICING,
    #         #     start_date
    #         # )
            
    #         orders.new_create(
    #             users,
    #             LOCALES,
    #             products,
    #             num_orders,
    #             MAX_ITEMS_PER_ORDER,
    #             start_date, 
    #             end_date
    #         )

            # start_date += timedelta(days=7)
            # end_date += timedelta(days=7)


        # users.to_csv()
        # products.to_csv()
        # orders.to_csv()
                
        # users.to_csv(start_date, end_date)
        # products.to_csv(start_date, end_date)
        # orders.to_csv(start_date, end_date)

    # except Exception as e:
        
    #     print(e)