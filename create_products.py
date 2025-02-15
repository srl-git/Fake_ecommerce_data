from ecommerce import Ecommerce
import random
from datetime import datetime


DB_PATH = 'database/ecommerce_data.db'

LABEL_PREFIXES = ['LCR', 'SUMO', 'STDR', 'KALA', 'GAS']

PRICING = [18.0, 19.0,
           20.0, 21.0]


def main():

    ecommerce = Ecommerce(DB_PATH)
    
    num_items = random.randint(2, 4)
    label_prefix = random.choice(LABEL_PREFIXES)

    ecommerce.products.create(
                label_prefix,
                num_items,
                PRICING
            )
    
    ecommerce.products.to_csv(
        start_date = datetime.now(),
        local_file = False,
        cloud_storage_file = True
    )

if __name__ == '__main__':

    main()