from datetime import datetime, timedelta
from unidecode import unidecode
import sqlite3
import random
import math
import csv

from faker import Faker

import sql_statements as sql


LABEL_PREFIX = 'STDR'
NUM_PRODUCTS = 25
PRICING = [18.0,19.0,20.0,21.0]

NUM_USERS = 900
LOCALES = ['en_GB','en_US','fr_FR','en_CA','de_DE']

NUM_ORDERS = 20
MAX_ITEMS_PER_ORDER = 7
ORDERS_START_DATE = datetime.strptime('2025-01-18','%Y-%m-%d')
MESSY_DATA = False


db_path = 'ecommerce_data.db'


class DatabaseConnection:

    def __init__(self, path: str) -> None:

        self.path = path

    def __enter__(self):

        self.connection = sqlite3.connect(self.path)
        self.cursor = self.connection.cursor()

        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        
        try:
            self.connection.commit()
    
        except sqlite3.Error as e:
            self.connection.rollback()
            print(f'Transaction failed and rolled back: {e}')

        finally:
            if self.connection:
                self.connection.close()
  

class Products:
    
    def __init__(self, db_path: str) -> None:

        self.db_path = db_path
        self._initialise_db_table()
        
    def create(self, label_prefix: str, num_items: int, pricing: list[float]) -> None :
        
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
    
    def update(self, sku: str | list[str], price: int | float | list[int | float] | None = None, active: bool | list[bool] | None = None) -> None:

        products_to_update = self.get_products(sku)

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

            with DatabaseConnection(self.db_path) as db:
                db.cursor.executemany(sql.product_statements.update_products,(update_data))

    def get_products(self, sku: str | list[str] | tuple[str] | None = None) -> list[tuple]:
        
        with DatabaseConnection(self.db_path) as db:

            if sku:
                if isinstance(sku, str):
                    sku = (sku, )
                db.cursor.execute(sql.product_statements.get_products_by_sku(sku), sku)
                products = db.cursor.fetchall()

                return products
            
            else:
                db.cursor.execute(sql.product_statements.get_products)
                all_products = db.cursor.fetchall()

                return all_products
    
    def get_last_added(self) -> tuple:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.product_statements.get_last_added)
            last_added = db.cursor.fetchall()[0]

        return last_added

    def get_last_updated(self) -> tuple:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.product_statements.get_last_added)
            last_added = db.cursor.fetchall()[0]

        return last_added
    
    def to_csv(self) -> None:

        date_today = datetime.today().strftime('%Y-%m-%d')
        file_path = f'Product_report_{date_today}.csv'
        export_data = [product[:-1] for product in self.get_products()]

        with open(file_path, mode='w', newline='') as file:
            
            writer = csv.writer(file)
            writer.writerow(['Product SKU', 'Price', 'Date Created', 'Date Updated', 'Active'])
            
            for row in export_data:
                writer.writerow(row)

    def _initialise_db_table(self) -> None:
        
        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.product_statements.create_product_table)

    def _drop_db_table(self) -> None:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.product_statements.drop_product_table)

    def _get_sku_index(self, label_prefix: str) -> int:
        
        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.product_statements.get_sku_index, (f'{label_prefix}%',))
            sku_index = db.cursor.fetchone()[0]

        return sku_index

    def _get_upper_limit(self) -> float:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.product_statements.get_upper_limit)
            max_popularity_score = db.cursor.fetchone()[0]

        upper_limit = max_popularity_score * 1.1 if max_popularity_score else 1
        
        return upper_limit

    def _set_popularity_scores(self) -> None:
 
        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.product_statements.get_popularity_scores)
            old_popularity_scores = tuple(score[0] for score in db.cursor.fetchall())

        normalizer = 1 / float(sum(old_popularity_scores))
        new_popularity_scores = tuple(score * normalizer for score in old_popularity_scores)
        update_scores = zip(new_popularity_scores, old_popularity_scores)

        with DatabaseConnection(self.db_path) as db:
            db.cursor.executemany(sql.product_statements.set_popularity_scores, (update_scores))

    def _add_to_db(self, products: list[tuple]) -> None:
            
        with DatabaseConnection(self.db_path) as db:
            db.cursor.executemany(sql.product_statements.add_products_to_db, products)
        
        self._set_popularity_scores()


class Users:

    def __init__(self, db_path) -> None:
        
        self.db_path = db_path
        self._initialise_db_table()
    
    def create(self, num_users: int, locales: list[str]) -> None: 

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

    def update(self, user_id: int | list[int] | tuple[int, ...], user_name: str | list[str] | None = None, user_address: str | list[str] | None = None, user_country: str | list[str] | None = None, user_email: str | list[str] | None = None) -> None:
        
        if isinstance(user_id, int):
            user_id = (user_id, )

        users_to_update = self.get_users(user_id)

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

            with DatabaseConnection(self.db_path) as db:
                db.cursor.executemany(sql.user_statements.update_users,(update_data))

    def get_users(self, user_id: int | list[int] | tuple[int] | None = None) -> list[tuple]:
        
        with DatabaseConnection(self.db_path) as db:
            if user_id:
                if isinstance(user_id, int):
                    user_id = (user_id, )
                db.cursor.execute(sql.user_statements.get_users_by_id(user_id), user_id)
                users = db.cursor.fetchall()
                return users
            else:    
                db.cursor.execute(sql.user_statements.get_users)
                all_users = db.cursor.fetchall()
                return all_users

    def get_last_added(self) -> tuple:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.user_statements.get_last_added)
            last_added = db.cursor.fetchall()[0]

        return last_added

    def get_last_updated(self) -> tuple:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.user_statements.get_last_updated)
            last_added = db.cursor.fetchall()[0]

        return last_added
    
    def to_csv(self) -> None:

        date_today = datetime.today().date().strftime('%Y-%m-%d')
        file_path = f'User_report_{date_today}.csv'
        export_data = self.get_users()

        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['user_id', 'user_name', 'user_address', 'user_country', 'user_email', 'date_created', 'date_updated'])
            
            for row in export_data:
                writer.writerow(row)        
   
    def _create_email(self, name: str) -> str:
        email_prefix = unidecode(name.lower().replace(' ','').replace('.',''))
        email = f'{email_prefix}@example.com'

        return email
    
    def _initialise_db_table(self) -> None:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.user_statements.create_user_table)

    def _drop_db_table(self) -> None:
        
        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.user_statements.drop_user_table)

    def _add_to_db(self, users: list[tuple]):

        with DatabaseConnection(self.db_path) as db:
            db.cursor.executemany(sql.user_statements.add_users_to_db, users)


class Orders:
    
    def __init__(self, db_path: str) -> None:
        
        self.db_path = db_path
        self._initialise_db_table()

    def create(self, num_orders: int, users: Users, products: Products, max_num_items: int, start_date: datetime, end_date: str ='today') -> None:
        
        try:
            self.users = users.get_users()
            self.products = products.get_products()

            if not self.users or not self.products:
                raise ValueError("Can't generate orders without any users or products in the database.")
            
            self.num_orders = num_orders
            self.item_skus = [product[0] for product in self.products if product[4] == 1]
            # self.item_prices = [product[1] for product in self.products if product[4] == 1]
            self.item_prices = {product[0]: product[1] for product in self.products if product[4] == 1}
            self.item_popularities = [product[5] for product in self.products if product[4] == 1]
            self.max_num_items = max_num_items
            self.start_date = start_date
            self.end_date = datetime.today() if end_date == 'today' else datetime.strptime(end_date, '%Y-%m-%d')
            self.order_dates = self._get_random_dates()
            last_order_id = self._get_last_order_id()

            self.orders = []

            for i in range(self.num_orders):

                items_in_order = self._get_num_items()
                order_id = (last_order_id + 1) + i
                random_user_id = random.choice(self.users)[0]
                date_created = self.order_dates[i].date().strftime('%Y-%m-%d')
                date_updated = date_created
                
                order_lines = {}

                for j in range(items_in_order):

                    random_product = random.choices(self.item_skus,weights=self.item_popularities)[0]

                    if random_product in order_lines:
                        order_lines[random_product] += 1
                    else:
                        order_lines[random_product] = 1

                for item_sku, qty in order_lines.items():

                    order = (order_id, random_user_id, item_sku, qty, self.item_prices[item_sku], date_created, date_updated)
                    self.orders.append(order)
                
            self._add_to_db(self.orders)

        except Exception as e:
            print(f'Error: {e}')

            return

    # def introduce_messy_data(self,orders):

    #     for order in orders:

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
                            
    #     return tuple(messy_data)

    def to_csv(self) -> None:
        
        date_today = datetime.today().date().strftime('%Y-%m-%d')
        file_path = f'Order_report_{date_today}.csv'
        export_data = self.get_orders()

        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['order__line_id', 'order_id', 'user_id', 'item_sku', 'qty', 'item_price', 'date_created', 'date_updated'])
    
            for row in export_data:
                writer.writerow(row)

    def _get_random_dates(self) -> list[datetime]:
        
        date_range = (self.end_date - self.start_date).days
        order_dates = []
        current_date = self.start_date

        if date_range > 0:
            base_orders_per_day = self.num_orders // date_range
            orders_per_day = [base_orders_per_day] * date_range
            orders_per_day = [x + random.randint(-x // 2, x // 2) for x in orders_per_day]
            difference = self.num_orders - sum(orders_per_day)
        
            for _ in range(abs(difference)):

                idx = random.randint(0, len(orders_per_day) - 1)
                orders_per_day[idx] += 1 if difference > 0 else -1

            for day in orders_per_day:

                for i in range(day):
    
                    order_dates.append(current_date)
                    
                current_date += timedelta(days=1)
                
        elif date_range == 0:
            order_dates = [current_date] * self.num_orders

        else: 
            raise ValueError('Start date for orders cannot be in the future.') 

        return order_dates
    
    def _get_num_items(self) -> int:

        max_num_items_list = list(range(1, self.max_num_items + 1))
        scaling = 0.6
        inv_exp_list = [1 / math.exp(x * scaling) for x in max_num_items_list]
        total = sum(inv_exp_list)
        weighting = [value / total for value in inv_exp_list]
        num_items = random.choices(max_num_items_list,weights=weighting)[0]

        return num_items
    
    def _initialise_db_table(self) -> None:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.order_statements.create_order_table)
    
    def _drop_db_table(self) -> None:
    
        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.order_statements.drop_order_table) 

    def _add_to_db(self, orders: list[tuple]) -> None:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.executemany(sql.order_statements.add_orders_to_db, orders)

    def _get_last_order_id(self) -> int:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.order_statements.get_last_order_id)
            result = db.cursor.fetchone()
            last_order_id = result[0] if result is not None else 0
            
            return last_order_id

    def get_orders(self) -> list[tuple]:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.order_statements.get_orders)
            all_orders = db.cursor.fetchall()

            return all_orders
    
products = Products(db_path)
# products.create(LABEL_PREFIX,NUM_PRODUCTS, PRICING)
# products.update('STDR014',price=5,active=False)
# products.update(['STDR001','STDR002','STDR003','STDR004','STDR005'],active=False)
# products.get_last_added()
# products.get_last_updated()
# products._get_sku_index('STDR')
# products._get_upper_limit()
# all_products = products.get_products()
# print(all_products)
# products.to_csv()
# products._drop_db_table()

users = Users(db_path)
# users.create(NUM_USERS,LOCALES)
# users.update(7,'Harry Linger')
# print(users.get_users([1,2,3,4,5]))
# all_users = users.get_users()
# users.to_csv()
# users._drop_db_table()

orders = Orders(db_path)
# orders.create(num_orders=NUM_ORDERS,users=users,products=products,max_num_items=MAX_ITEMS_PER_ORDER,start_date=ORDERS_START_DATE)
# all_orders = orders.get_orders()
# orders.to_csv()
# orders._drop_db_table()
