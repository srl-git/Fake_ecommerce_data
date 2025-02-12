from datetime import datetime, timedelta
from ecommerce import Ecommerce, Products, Users, Orders
import random
import cProfile

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

def main():

    try:

        ecommerce = Ecommerce(DB_PATH)

        start_date = (datetime.today() - timedelta(days = 2100))
        end_date = start_date + timedelta(days=7)
        
        # num_items = random.randint(1, 3)

        # ecommerce.products.create(
        #             LABEL_PREFIX,
        #             num_items,
        #             PRICING
        #         )

        num_orders = 10000

            # users.create(
            #     5,
            #     LOCALES,
            #     start_date
            # )
            
            
        # ecommerce.orders.new_create(
        #     ecommerce.users,
        #     LOCALES,
        #     ecommerce.products,
        #     num_orders,
        #     MAX_ITEMS_PER_ORDER
        # )

        ecommerce.create_orders_and_users(
            LOCALES,
            num_orders,
            MAX_ITEMS_PER_ORDER,
            start_date,
            end_date
        )

        start_date += timedelta(days=7)
        end_date += timedelta(days=7)
                
        print(ecommerce)
        # users.to_csv(start_date, end_date)
        # products.to_csv(start_date, end_date)
        # orders.to_csv(start_date, end_date)

    except Exception as e:
        
        print(e)


if __name__ == '__main__':

   cProfile.run('main()',sort='tottime')
