from datetime import datetime, timedelta
from unidecode import unidecode
import csv
import random

from faker import Faker
import sqlite3

import sql_statements as sql


LABEL_PREFIX = 'TEST'
NUM_PRODUCTS = 10
PRICING = [18.0,19.0,20.0,21.0]

NUM_USERS = 45
LOCALES = ['en_GB','en_US','fr_FR','en_CA','de_DE']

NUM_ORDERS = 50
ORDERS_START_DATE = datetime.strptime('2024-01-18','%Y-%m-%d')
MESSY_DATA = True

conn = sqlite3.connect("ecommerce_data.db")
cursor = conn.cursor()

class Products:
    
    def __init__(self):

        self._initialise_db_table()
        
    def create(self, label_prefix: str, num_items: int, pricing: list[float]):
        
        products = []
        index = self._get_sku_index(label_prefix)
        date_created = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        date_updated = date_created
        
        for i in range(num_items):

            sku = f'{label_prefix}{index + 1 + i :03}'
            price = random.choice(pricing)
            upper_limit = self._get_upper_limit()
            weighting = random.uniform(0.0, upper_limit)
            active = True
            product = (sku, price, date_created, date_updated, active, weighting)
            products.append(product)

        self._add_to_db(products)

        return self
    
    def update(self, sku: str | list[str] | tuple[str], price: int | float | list[int | float] | None = None,active: bool | list[bool] | None = None):

        if isinstance(sku, str):
            sku = (sku, )

        cursor.execute(sql.product_statements.get_products_by_sku(sku), sku)
        products_to_update = cursor.fetchall()

        if isinstance(price, (int, float)):
            price = [price] * len(products_to_update)

        if isinstance(active, bool):
            active = [active] * len(products_to_update)

        update_data = []

        if products_to_update:
            for product in products_to_update:

                sku_to_update = product[0]
                index = sku.index(sku_to_update)
                price_updated = price[index] if price is not None else product[1]
                date_updated = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                active_updated = active[index] if active is not None else product[4]
                update_data.append((price_updated, date_updated, active_updated, sku_to_update))

            cursor.executemany(sql.product_statements.update_products,(update_data))
            conn.commit()

    def get_products(self):
        
        cursor.execute(sql.product_statements.get_products)
        all_products = cursor.fetchall()
        
        return all_products
    
    def get_last_added(self):

        cursor.execute(sql.product_statements.get_last_added)
        last_added = cursor.fetchall()[0]

        return last_added

    def get_last_updated(self):

        cursor.execute(sql.product_statements.get_last_added)
        last_added = cursor.fetchall()[0]

        return last_added
    
    def to_csv(self):

        date_today = datetime.today().strftime('%Y-%m-%d')
        file_path = f'Product_report_{date_today}.csv'
        export_data = [product[:-1] for product in products.get_products()]

        with open(file_path, mode='w', newline='') as file:
            
            writer = csv.writer(file)
            writer.writerow(['Product SKU', 'Price', 'Date Created', 'Date Updated', 'Active'])
            
            for row in export_data:
                writer.writerow(row)

    def _initialise_db_table(self):
        
        cursor.execute(sql.product_statements.create_product_table)

    def _drop_db_table(self):

        cursor.execute(sql.product_statements.drop_product_table)

    def _get_sku_index(self, label_prefix):
        
        cursor.execute(sql.product_statements.get_sku_index, (f'{label_prefix}%',))
        sku_index = cursor.fetchone()[0]

        return sku_index

    def _get_upper_limit(self):

        cursor.execute(sql.product_statements.get_upper_limit)
        max_popularity_score = cursor.fetchone()[0]
        upper_limit = max_popularity_score * 1.1 if max_popularity_score else 1
        
        return upper_limit

    def _set_popularity_scores(self):
 
        cursor.execute(sql.product_statements.get_popularity_scores)
        old_popularity_scores = tuple(score[0] for score in cursor.fetchall())
        normalizer = 1 / float(sum(old_popularity_scores))
        new_popularity_scores = tuple(score * normalizer for score in old_popularity_scores)
        update_scores = zip(new_popularity_scores, old_popularity_scores)
        cursor.executemany(sql.product_statements.set_popularity_scores, (update_scores))

    def _add_to_db(self, products):
            
        cursor.executemany(sql.product_statements.add_products_to_db, products)
        self._set_popularity_scores()
        conn.commit()


class Users:

    def __init__(self):

        self._initialise_db_table()
    
    def create(self, num_users: int, locales: list[str]): 

        users = []

        for i in range(num_users):

            random_locale = random.choice(locales)
            fake = Faker(random_locale)
            profile = fake.simple_profile()
            user_name = profile['name']
            user_address = profile['address'].replace('\n',', ')
            user_country = fake.current_country()
            user_email = self._create_email(user_name)
            date_created = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            date_updated = date_created
            user = (user_name, user_address, user_country, user_email, date_created, date_updated)
            users.append(user)

        self._add_to_db(users)

        return self

    def update(self, user_id: int | list[int] | tuple[int, ...], user_name: str | list[str] | None = None, user_address: str | list[str] | None = None, user_country: str | list[str] | None = None, user_email: str | list[str] | None = None):
        
        if isinstance(user_id, int):
            user_id = (user_id, )

        cursor.execute(sql.user_statements.get_users_by_id(user_id), user_id)
        users_to_update = cursor.fetchall()


        if isinstance(user_name, str):
            user_name = [user_name] * len(users_to_update)

        if isinstance(user_address, str):
            user_address = [user_address] * len(users_to_update)

        if isinstance(user_country, str):
            user_country = [user_country] * len(users_to_update)

        if isinstance(user_email, str):
            user_email = [user_email] * len(users_to_update)

        update_data = []

        if users_to_update:
            for user in users_to_update:

                user_id_to_update = user[0]
                index = user_id.index(user_id_to_update)
                user_name_updated = user_name[index] if user_name is not None else user[1]
                user_address_updated = user_address[index] if user_address is not None else user[2]
                user_country_updated = user_country[index] if user_country is not None else user[3]
                user_email_updated = user_email[index] if user_email is not None else user[4]
                date_updated = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                update_data.append((user_name_updated, user_address_updated, user_country_updated, user_email_updated, date_updated, user_id_to_update))

            cursor.executemany(sql.user_statements.update_users,(update_data))
            conn.commit()

    def get_users(self):
        
        cursor.execute(sql.user_statements.get_users)
        all_users = cursor.fetchall()
        
        return all_users

    def get_last_added(self):

        cursor.execute(sql.user_statements.get_last_added)
        last_added = cursor.fetchall()[0]

        return last_added

    def get_last_updated(self):

        cursor.execute(sql.user_statements.get_last_updated)
        last_added = cursor.fetchall()[0]

        return last_added
    
    def to_csv(self):

        date_today = datetime.today().date().strftime('%Y-%m-%d')
        file_path = f'User_report_{date_today}.csv'
        export_data = self.get_users()

        with open(file_path, mode='w', newline='') as file:
            
            writer = csv.writer(file)
            writer.writerow(['user_id', 'user_name', 'user_address', 'user_country', 'user_email', 'date_created', 'date_updated'])
            
            for row in export_data:
                writer.writerow(row)        
   
    def _create_email(self,name):
        email_prefix = unidecode(name.lower().replace(' ','').replace('.',''))
        email = f'{email_prefix}@example.com'
        return email
    
    def _initialise_db_table(self):

        cursor.execute(sql.user_statements.create_user_table)

    def _drop_db_table(self):
        
        cursor.execute(sql.user_statements.drop_user_table)
        conn.commit()

    def _add_to_db(self, users):
             
        cursor.executemany(sql.user_statements.add_users_to_db, users)
        conn.commit()


# class Orders:

#     def __init__(self, num_orders: int, users: Users, products: Products, max_num_items: int, start_date: datetime, end_date: str ='today', messy_data: bool =False):
        
#         self.num_orders = num_orders
#         self.users = users
#         self.products = products
#         self.max_num_items = max_num_items
#         self.start_date = start_date
#         self.end_date = datetime.today() if end_date == 'today' else datetime.strptime(end_date, '%Y-%m-%d')

#         self.detailed_orders = []
#         self.simple_orders = []

#         self.order_dates = self.get_random_dates()

#         for i in range(self.num_orders):

#             items_in_order = random.randint(1,self.max_num_items)
        
#             order_date = self.order_dates[i].date()

#             random_customer = random.choice(self.users.users)
            
#             (
#                 customer_id, 
#                 customer_name, 
#                 customer_address,
#                 customer_country, 
#                 customer_email
#             ) = random_customer

#             order_id = f'{(i + 1):04}'

#             order_lines = {}

#             for _ in range(items_in_order):

#                 random_product = random.choices(products.skus,weights=self.products.popularity)[0]

#                 if random_product in order_lines:
#                     order_lines[random_product] += 1
#                 else:
#                     order_lines[random_product] = 1

#             for sku, qty in order_lines.items():

#                 detailed_order = (order_id, order_date, customer_id, customer_name, customer_address, customer_country, customer_email, sku, qty, products.price[sku])
                
#                 if messy_data:
#                     detailed_order = self.introduce_messy_data(detailed_order)
#                     if random.random() < 0.05:
#                         self.detailed_orders.append(detailed_order)

#                 self.detailed_orders.append(detailed_order)

#                 simple_order = (order_id, order_date, customer_id, sku, qty, products.price[sku])

#                 if messy_data:
#                     simple_order = self.introduce_messy_data(simple_order)
#                     if random.random() < 0.05:
#                         self.simple_orders.append(simple_order)

#                 self.simple_orders.append(simple_order)

#     def get_random_dates(self):
        
#         date_range = (self.end_date - self.start_date).days
#         order_dates = []
#         current_date = self.start_date

#         if date_range > 0:

#             base_orders_per_day = self.num_orders // date_range
#             orders_per_day = [base_orders_per_day] * date_range
#             orders_per_day = [x + random.randint(-x // 2, x // 2) for x in orders_per_day]
#             difference = self.num_orders - sum(orders_per_day)
        
#             for _ in range(abs(difference)):
#                 idx = random.randint(0, len(orders_per_day) - 1)
#                 orders_per_day[idx] += 1 if difference > 0 else -1

#             for day in orders_per_day:

#                 for _ in range(day):
#                     order_dates.append(current_date)
                
#                 current_date += timedelta(days=1)
                
#         elif date_range == 0:

#             order_dates = [current_date] * self.num_orders

#         else: raise ValueError('Date cannot be in the future.') 

#         return order_dates

#     def introduce_messy_data(self,order):

#         messy_data = list(order)
        
#         if random.random() < 0.05:  
#             if random.random() < 0.5:
#                 messy_data[1] = messy_data[1].strftime('%d/%m/%Y')
#             else:
#                 messy_data[1] = messy_data[1].strftime('%d-%m-%Y')

#         if random.random() < 0.1:
#                 idx = random.randint(0, len(order) - 4)
#                 messy_data[idx] = None

#         if random.random() < 0.2:
#                 idx = random.randint(0, len(order) - 4)
#                 if type(messy_data[idx]) is str:
#                     if random.random() < 0.5:
#                         messy_data[idx] = messy_data[idx].lower()
#                     else:
#                         messy_data[idx] = messy_data[idx].upper()
                        
#         return tuple(messy_data)

#     def to_csv(self,detailed=True):

#         date_today = datetime.today().date().strftime('%Y-%m-%d')
#         file_path = f'Order_report_{date_today}.csv'

#         with open(file_path, mode='w', newline='') as file:
            
#             writer = csv.writer(file)
    
#             if detailed:
#                 writer.writerow(['Order ID', 'Order Date', 'Customer ID', 'Name', 'Address', 'Country', 'Email', 'Item', 'Qty', 'Price'])
#                 for row in self.detailed_orders:
#                     writer.writerow(row)
#             else:
#                 writer.writerow(['Order ID', 'Order Date', 'Customer ID', 'Item', 'Qty', 'Price'])
#                 for row in self.simple_orders:
#                     writer.writerow(row)

products = Products()
products.create(LABEL_PREFIX,NUM_PRODUCTS, PRICING)
# products._drop_db_table()

users = Users()
users.create(NUM_USERS,LOCALES)
# users._drop_db_table()


# orders = Orders(num_orders=NUM_ORDERS, 
#                 users=customers, 
#                 products=products, 
#                 max_num_items=5, 
#                 start_date=ORDERS_START_DATE,
#                 messy_data=MESSY_DATA
#                 )

# products.to_csv()
# customers.to_csv()
# orders.to_csv()