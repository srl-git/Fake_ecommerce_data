from ecommerce_data_generator.Ecommerce import Ecommerce
import random
from datetime import datetime

LABEL_PREFIXES = [
    'LCR', 'SUMO',
    'STDR', 'KALA', 
    'GAS', 'PIL'
]

PRICING = [
    18.0, 19.0,
    20.0, 21.0
]

LOCALES = [
    'en_GB', 'en_US', 'fr_FR',
    'en_CA', 'de_DE', 'en_AU', 
    'es_ES', 'fr_BE', 'it_IT', 
    'ja_JP', 'nl_NL', 'pt_PT'
]

def main():

    ecommerce = Ecommerce()

    wednesday = datetime.now().isoweekday() == 3

    if wednesday:
        ecommerce.products.create(
                    label_prefix=random.choice(LABEL_PREFIXES),
                    num_items=random.randint(1, 4),
                    pricing=PRICING,
                )
        
    ecommerce.create_orders_and_users(
        locales=LOCALES,
        num_orders=random.randint(3, 500),
        max_num_items=7
    )

    ecommerce.to_csv(
        start_date=datetime.now(),
        messy_data=True,
        local_file=False,
        cloud_storage_file=True
    )

    print(ecommerce)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
