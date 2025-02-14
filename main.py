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

    ecommerce = Ecommerce(DB_PATH)
    
    # num_items = random.randint(1, 3)

    ecommerce.products.create(
                LABEL_PREFIX,
                25,
                PRICING
            )

    num_orders = 10000
        
    ecommerce.create_orders_and_users(
        LOCALES,
        100,
        MAX_ITEMS_PER_ORDER
    )
            
    # ecommerce.to_csv()
    

if __name__ == '__main__':
    main()
#    cProfile.run('main()',sort='tottime')
    # try:

    # except Exception as e:
        
    #     print(e)