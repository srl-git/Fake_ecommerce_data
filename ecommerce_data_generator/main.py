import random
from datetime import datetime

import yaml

from ecommerce_data_generator.Ecommerce import Ecommerce
import logger

log = logger.get_logger(__name__)


def main():

    with open('ecommerce_data_generator/config.yaml', 'rt') as f:
            config = yaml.safe_load(f.read())

    ecommerce = Ecommerce()

    wednesday = datetime.now().isoweekday() == 3

    if wednesday:
        ecommerce.products.create(
                    num_items=random.randint(1, 6),
                    label_prefix=random.choice(config.get('label_prefix')),
                    pricing=config.get('pricing'),
        )
        
    ecommerce.create_orders(
        num_orders=random.randint(3, 300),
        **config.get('create_orders_and_users')
    )

    ecommerce.to_csv(
        start_date=datetime.now(),
        **config.get('to_csv')
    )

    print(ecommerce)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
