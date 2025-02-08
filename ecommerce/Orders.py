from datetime import datetime, timedelta
import random
import math
import csv

from database.DatabaseConnection import DatabaseConnection
from ecommerce.Products import Products
from ecommerce.Users import Users
import sql_statements as sql

class Orders:
    
    def __init__(self, db_path: str) -> None:
        
        self.db_path = db_path
        self._initialise_db_table()

    def __repr__(self) -> str:
        
        return f'Orders({self.db_path})'

    def __str__(self) -> str:
        
        return f'There are {self.get_count_orders()} orders in the database'
    
    def create(
        self, 
        users: Users, 
        products: Products,
        num_orders: int,
        max_num_items: int,
        start_date: str | datetime,
        end_date: str | datetime = datetime.now()
    ) -> None:
        
        try:
            self.users = users.get_users()
            self.products = products.get_products()
            if len(self.products) < 1 or len(self.users) < 1:
                raise ValueError("ERROR in Orders.create(): Can only generate orders with at least 1 user and product in the database.")

        except:
            raise ValueError("ERROR in Orders.create(): Can't generate orders without any users or products in the database.")

        self._validate_create_args(num_orders, max_num_items, start_date, end_date)

        self.num_orders = num_orders
        self.item_skus = [product[0] for product in self.products if product[5] == 1]
        self.item_prices = {product[0]: product[1] for product in self.products if product[5] == 1}
        self.item_popularities = [product[6] for product in self.products if product[5] == 1]
        self.user_ids = [user[0] for user in self.users]
        self.user_popularities = [user[7] for user in self.users]
        self.max_num_items = max_num_items
        self.start_date = start_date if isinstance(start_date, datetime) else datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = end_date if isinstance(end_date, datetime) else datetime.strptime(end_date, '%Y-%m-%d')
        self.order_dates = self._get_random_dates()
        last_order_id = self._get_last_order_id()

        self.orders = []

        for i in range(self.num_orders):

            items_in_order = self._get_num_items()
            order_id = (last_order_id + 1) + i
            random_user_id = random.choices(self.user_ids, self.user_popularities)[0]
            date_created = self.order_dates[i].date().strftime('%Y-%m-%d')
            date_updated = date_created
            
            order_lines = {}

            for j in range(items_in_order):

                random_product = random.choices(self.item_skus, self.item_popularities)[0]

                if random_product in order_lines:
                    order_lines[random_product] += 1
                else:
                    order_lines[random_product] = 1

            for item_sku, qty in order_lines.items():

                order = (order_id, random_user_id, item_sku, qty, self.item_prices[item_sku], date_created, date_updated)
                self.orders.append(order)
            
        self._add_to_db(self.orders)


    def get_count_orders(self) -> int:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.order_statements.get_count_orders)
            count_orders = db.cursor.fetchone()[0]
        
        return count_orders
    
    def get_orders(self) -> list[tuple]:

        with DatabaseConnection(self.db_path) as db:
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
        
        with DatabaseConnection(self.db_path) as db:
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
                    messy_order[6] = datetime.strptime(messy_order[6],'%Y-%m-%d').strftime('%d/%m/%Y')
                    messy_order[7] = datetime.strptime(messy_order[7],'%Y-%m-%d').strftime('%d/%m/%Y')
                else:
                    messy_order[6] = datetime.strptime(messy_order[6], '%Y-%m-%d').strftime('%d-%m-%Y')
                    messy_order[7] = datetime.strptime(messy_order[7], '%Y-%m-%d').strftime('%d-%m-%Y')

            # Introduce blank values
            if random.random() < 0.1:
                    idx = random.randint(5, 7)
                    messy_order[idx] = None

            # Duplicate order rows
            if random.random() < 0.02:
                messy_orders.append(messy_order)

            # Change string case
            # if random.random() < 0.2:
            #         idx = random.randint(0, len(order) - 4)
            #         if type(messy_order[idx]) is str:
            #             if random.random() < 0.5:
            #                 messy_order[idx] = messy_order[idx].lower()
            #             else:
            #                 messy_order[idx] = messy_order[idx].upper()
            
            messy_orders.append(messy_order)             
        
        return messy_orders

    def to_csv(
        self,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        messy_data: bool = False
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

        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['order_line_id', 'order_id', 'user_id', 'item_sku', 'qty', 'item_price', 'date_created', 'date_updated'])
    
            for row in export_data:
                writer.writerow(row)

    def _get_random_dates(self) -> list[datetime]:
        
        date_range = (self.end_date.date() - self.start_date.date()).days
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
    
    def _validate_create_args(
            self, 
            num_orders: int, 
            max_num_items: int, 
            start_date: str | datetime, 
            end_date: str | datetime
    ) -> None:

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
        
        try:
            if not isinstance(start_date, datetime):
                datetime.fromisoformat(start_date)

            if not isinstance(end_date, datetime):
                datetime.fromisoformat(end_date)

        except ValueError:
            raise ValueError(
                'ERROR in Orders.create(). '
                'Expected a datetime object or valid date string in format YYYY-MM-DD for start_date and end_date arguments.'
            )