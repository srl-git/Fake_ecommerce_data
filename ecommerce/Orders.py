from datetime import datetime
import random
import math
import csv
import io
import os

from google_cloud import CloudSQLConnection, upload_to_bucket
from ecommerce.Products import Products
from ecommerce.Users import Users
import sql_statements as sql

class Orders:
    
    def __init__(self) -> None:
        
        self._initialise_db_table()

    def __repr__(self) -> str:
        
        return f'Orders()'

    def __str__(self) -> str:
        
        return f'There are {self.get_count_orders()} orders in the database.'
    
    def create(
        self, 
        users: Users,
        locales: list[str],
        products: Products,
        num_orders: int,
        max_num_items: int
    ) -> None:
        
        self._validate_create_args(users, products, num_orders, max_num_items)

        items = self._get_active_products(products)
        all_user_ids = self._get_user_ids(users, locales, num_orders)
        orders = self._generate_order_lines(items, all_user_ids, num_orders, max_num_items)
        
        self._add_to_db(orders)

        print(f'Adding {len(orders)} order line(s) to the database.')

    def _get_active_products(self, products: Products) -> dict:

        try:
            all_products = products.get_products()
            if len(all_products) < 1:
                raise ValueError
        except:
            raise ValueError("ERROR in Orders.create(): Can't generate orders without any products in the database.")
        
        return {
            product[0]: {'item_price': product[1], 'item_popularity': product[6] } 
            for product in all_products if product[5] == 1
        }
    
    def _get_user_ids(self, users: Users, locales: list[str], num_orders: int) -> list[int]:

        ratio_previous_users = random.uniform(0.0, 0.3)
        num_previous_users = round(num_orders * ratio_previous_users)
        previous_users = users._get_random_users(num_previous_users)
        num_new_users = num_orders - len(previous_users)
        new_users = users.create(num_new_users, locales)
        all_users = previous_users + new_users
        random.shuffle(all_users)
        
        return [user[0] for user in all_users]

    def _generate_order_lines(self, items: dict, user_ids: list[int], num_orders: int, max_num_items: int) -> list[tuple]:

        item_skus = [item_sku for item_sku in items]
        item_popularities = [details['item_popularity'] for item_sku, details in items.items()]
        last_order_id = self._get_last_order_id()

        orders = []

        for i in range(num_orders):

            items_in_order = self._get_random_num_items(max_num_items)
            order_id = (last_order_id + 1) + i
            user_id = user_ids[i]
            date_created = datetime.now().strftime('%Y-%m-%d')
            
            order_lines = {}

            for j in range(items_in_order):

                random_product = random.choices(item_skus, item_popularities)[0]

                if random_product in order_lines:
                    order_lines[random_product] += 1
                else:
                    order_lines[random_product] = 1

            for item_sku, qty in order_lines.items():

                price = items[item_sku]['item_price']
                order_line = (order_id, user_id, item_sku, qty, price, date_created)
                orders.append(order_line)
        
        return orders
                
    def get_count_orders(self) -> int:

        with CloudSQLConnection() as db:
            db.cursor.execute(sql.order_statements.get_count_orders)
            count_orders = db.cursor.fetchone()[0]
        
        return count_orders
    
    def get_orders(self) -> list[tuple]:

        with CloudSQLConnection() as db:
            db.cursor.execute(sql.order_statements.get_orders)
            all_orders = db.cursor.fetchall()

            return all_orders
    
    def get_orders_by_date_range(
        self,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None
    ) -> list[tuple]:

        start_date = '0000-01-01' if start_date is None else start_date
        end_date = datetime.today() if end_date is None else end_date

        if isinstance(start_date, datetime):
            start_date = start_date.strftime('%Y-%m-%d')
        if isinstance(end_date, datetime):
            end_date = end_date.strftime('%Y-%m-%d')
            
        try:
            datetime.fromisoformat(start_date)
            datetime.fromisoformat(end_date)
        except ValueError:
            raise ValueError(
                'ERROR in Orders.get_orders_by_date_range(). '
                'Expected a datetime object or a valid date string in format YYYY-MM-DD for start_date and end_date arguments.'
            )
        
        with CloudSQLConnection() as db:
            db.cursor.execute(sql.order_statements.get_orders_by_date_range,(start_date, end_date))
            orders_by_date_range = db.cursor.fetchall()
            
        return orders_by_date_range
    
    def _introduce_messy_data(self,orders):

        messy_orders = []

        for order in orders:

            messy_order = list(order)
            
            # Change date strings
            if random.random() < 0.05:  
                if random.random() < 0.5:
                    messy_order[6] = messy_order[6].strftime('%d/%m/%Y')
                else:
                    messy_order[6] = messy_order[6].strftime('%d-%m-%Y')

            # Introduce blank values
            if random.random() < 0.1:
                    idx = random.randint(5, 6)
                    messy_order[idx] = None

            # Duplicate order rows
            if random.random() < 0.02:
                messy_orders.append(messy_order)

            messy_orders.append(messy_order)             
        
        return messy_orders

    def to_csv(
        self,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        messy_data: bool = False,
        local_file: bool = True,
        cloud_storage_file: bool = False
    ) -> None:
              
        if start_date or end_date:
            try:
                export_data = self.get_orders_by_date_range(start_date, end_date)

                if isinstance(start_date, datetime):
                    start_date = start_date.strftime('%Y-%m-%d')
                if isinstance(end_date, datetime):
                    end_date = end_date.strftime('%Y-%m-%d')

                start_date = start_date if start_date else ''
                end_date = f'_{end_date}' if end_date else ''
                file_path = f'Order_report_{start_date}{end_date}.csv'
            
            except ValueError:
                raise ValueError(
                    'ERROR in Orders.to_csv(). '
                    'Expected a datetime object or a valid date string in format YYYY-MM-DD for start_date and end_date arguments.'
                )
        else:
            export_data = self.get_orders()
            date_today = datetime.today().date().strftime('%Y-%m-%d')
            file_path = f'Order_report_{date_today}.csv'

        if messy_data:
            export_data = self._introduce_messy_data(export_data)
        if len(export_data) == 0:
            return
        if local_file:
            self._save_to_file(export_data, file_path)
        if cloud_storage_file:
            self._save_to_cloud_storage(export_data, file_path)
    
    def _save_to_file(self, export_data: list[tuple], file_path: str):

        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['order_line_id', 'order_id', 'user_id', 'item_sku', 'qty', 'item_price', 'date_created'])
    
            for row in export_data:
                writer.writerow(row)
    
    def _save_to_cloud_storage(self, export_data: list[tuple], file_path: str):

        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(['order_line_id', 'order_id', 'user_id', 'item_sku', 'qty', 'item_price', 'date_created'])

        for row in export_data:
            writer.writerow(row)
        
        upload_data = csv_buffer.getvalue()

        upload_to_bucket(f'order_reports/{file_path}', upload_data, os.getenv('STORAGE_BUCKET_NAME'))
            
    def _get_random_num_items(self, max_num_items) -> int:

        max_num_items_list = list(range(1, max_num_items + 1))
        scaling = 0.6
        inv_exp_list = [1 / math.exp(x * scaling) for x in max_num_items_list]
        total = sum(inv_exp_list)
        weighting = [value / total for value in inv_exp_list]
        num_items = random.choices(max_num_items_list,weights=weighting)[0]

        return num_items
    
    def _initialise_db_table(self) -> None:

        with CloudSQLConnection() as db:
            db.cursor.execute(sql.order_statements.create_order_table)
    
    def _drop_db_table(self) -> None:
    
        with CloudSQLConnection() as db:
            db.cursor.execute(sql.order_statements.drop_order_table) 

    def _add_to_db(self, orders: list[tuple]) -> None:
        
        with CloudSQLConnection() as db:
            db.cursor.executemany(sql.order_statements.add_orders_to_db, orders)

    def _get_last_order_id(self) -> int:

        with CloudSQLConnection() as db:
            db.cursor.execute(sql.order_statements.get_last_order_id)
            result = db.cursor.fetchone()
            last_order_id = result[0] if result is not None else 0
            
        return last_order_id
    
    def _validate_create_args(
            self,
            users: Users,
            products: Products,
            num_orders: int, 
            max_num_items: int
    ) -> None:

        if not isinstance(users, Users):
            raise TypeError(
                'ERROR in Orders.create(). '
                'Expected an instance of Users for users argument. '
                f'Received {type(users).__name__}.'
                )

        if not isinstance(products, Products):
            raise TypeError(
                'ERROR in Orders.create(). '
                'Expected an instance of Products for products argument. '
                f'Received {type(products).__name__}.'
                )

        if not (isinstance(num_orders, int) and num_orders > 0) or isinstance(num_orders, bool):
            raise ValueError(
                'ERROR in Orders.create(). '
                'Expected a positive integer value for num_orders argument. '
                f'Received value "{num_orders}" of type: {type(num_orders).__name__}.'
                )

        if not (isinstance(max_num_items, int) and max_num_items > 0) or isinstance(max_num_items, bool):
            raise ValueError(
                'ERROR in Orders.create(). '
                'Expected a positive integer value for max_num_items argument. '
                f'Received value "{max_num_items}" of type: {type(max_num_items).__name__}.'
                )
        
