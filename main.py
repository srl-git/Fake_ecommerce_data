from ecommerce import Products, Users, Orders

LABEL_PREFIX = 'LCR'
NUM_ITEMS = 10
PRICING = [18.5,19.0,20.0,21.0]

NUM_USERS = 25
LOCALES = ['en_GB','en_US','fr_FR','en_CA','de_DE']

NUM_ORDERS = 500
MAX_ITEMS_PER_ORDER = 7
# ORDERS_START_DATE = datetime.strptime('2025-01-18','%Y-%m-%d')
ORDERS_START_DATE = '2025-01-18'
MESSY_DATA = False


db_path = 'ecommerce_data.db'

def main():

    products = Products(db_path)
    print(products)
    # products.create(label_prefix=LABEL_PREFIX,num_items=NUM_ITEMS,pricing=PRICING)
    # products.update(item_sku=['STDR014','LCR002'],item_price=3,is_active=True)
    # products.update(['LCR001','LCR002'], is_active=False)
    # print(products.get_last_added())
    # print(products.get_last_updated())
    # products._get_sku_index('STDR')
    # products._get_upper_limit()
    # all_products = products.get_products()
    # print(all_products)
    # date_products = products.get_products_by_date_range(start_date='2021-01-01', end_date=None)
    # print(date_products)
    # print(products.get_products_by_date_range(end_date='2025-01-15'))
    # products.to_csv()
    # print(products._get_upper_limit())
    # products._drop_db_table()

    users = Users(db_path)
    print(users)
    # print(len(users.get_users_by_date_range('2025-01-21')))
    # print(users.get_count_users())
    # users.create(NUM_USERS,LOCALES)
    # users.update(7,'Harry Linger')
    # print(users.get_users([1,2,3,4,5]))
    # all_users = users.get_users()
    # users.to_csv()
    # users._drop_db_table()

    orders = Orders(db_path)
    print(orders)
    # print(len(orders.get_orders_by_date_range(start_date='2024-12-21')))
    # print(orders)
    # orders.create(num_orders=NUM_ORDERS,users=users,products=products,max_num_items=MAX_ITEMS_PER_ORDER,start_date=ORDERS_START_DATE)
    # all_orders = orders.get_orders()
    # orders.to_csv(messy_data=True)
    # orders._drop_db_table()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)