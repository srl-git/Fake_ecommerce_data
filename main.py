from datetime import datetime, timedelta
from unidecode import unidecode
import csv
import random
from faker import Faker

LABEL_PREFIX = 'STDR'
NUM_PRODUCTS = 10
PRICING = [18.00,19.00,20.00,21.00]

NUM_USERS = 45
LOCALES = ['en_GB','en_US','fr_FR','en_CA','de_DE']

NUM_ORDERS = 50
ORDERS_START_DATE = datetime.strptime('2024-01-18','%Y-%m-%d')
MESSY_DATA = True


class Products:
    
    def __init__(self, label_prefix: str, num_items: int, pricing: list[float]):

        self.label_prefix = label_prefix
        self.num_items = num_items
        self.skus = [f'{self.label_prefix}{i:03}' for i in range(1,self.num_items + 1)]
        weighting = [random.random() for _ in range(self.num_items)]
        self.popularity = [w / sum(weighting) for w in weighting]
        self.prices = [random.choice(pricing) for i in range(0,self.num_items)]
        self.price = {sku: price for sku, price in zip(self.skus, self.prices)}
        
        self.products = []

        for i in range(num_items):
            product = (self.skus[i], self.prices[i])
            self.products.append(product)

    def to_csv(self) -> None:

        date_today: str = datetime.today().strftime('%Y-%m-%d')
        file_path: str = f'Product_report_{date_today}.csv'

        with open(file_path, mode='w', newline='') as file:
            
            writer = csv.writer(file)
            writer.writerow(['Product SKU', 'Price'])
            
            for row in self.products:
                writer.writerow(row)


class Users:

    def __init__(self, num_users: int, locales: list[str]):
        
        self.num_users = num_users
        self.locales = locales

        self.users = []

        for i in range(num_users):

            user_id = f'{i + 1:05}'
            random_locale = random.choice(locales)
            fake = Faker(random_locale)
            profile = fake.simple_profile()
            name = profile['name']
            address = profile['address'].replace('\n',', ')
            country = fake.current_country()
            email = self.create_email(name)
            user = (user_id, name, address, country, email)
            self.users.append(user)

    def create_email(self,name):
        email_prefix = unidecode(name.lower().replace(' ','').replace('.',''))
        email = f'{email_prefix}@example.com'
        return email
    
    def to_csv(self):

        date_today = datetime.today().date().strftime('%Y-%m-%d')
        file_path = f'Customer_report_{date_today}.csv'

        with open(file_path, mode='w', newline='') as file:
            
            writer = csv.writer(file)
            writer.writerow(['User ID', 'Name', 'Address', 'Country', 'Email'])
            
            for row in self.users:
                writer.writerow(row)            


class Orders:

    def __init__(self, num_orders: int, users: Users, products: Products, max_num_items: int, start_date: datetime, end_date: str ='today', messy_data: bool =False):
        
        self.num_orders = num_orders
        self.users = users
        self.products = products
        self.max_num_items = max_num_items
        self.start_date = start_date
        self.end_date = datetime.today() if end_date == 'today' else datetime.strptime(end_date, '%Y-%m-%d')

        self.detailed_orders = []
        self.simple_orders = []

        self.order_dates = self.get_random_dates()

        for i in range(self.num_orders):

            items_in_order = random.randint(1,self.max_num_items)
        
            order_date = self.order_dates[i].date()

            random_customer = random.choice(self.users.users)
            
            (
                customer_id, 
                customer_name, 
                customer_address,
                customer_country, 
                customer_email
            ) = random_customer

            order_id = f'{(i + 1):04}'

            order_lines = {}

            for _ in range(items_in_order):

                random_product = random.choices(products.skus,weights=self.products.popularity)[0]

                if random_product in order_lines:
                    order_lines[random_product] += 1
                else:
                    order_lines[random_product] = 1

            for sku, qty in order_lines.items():

                detailed_order = (order_id, order_date, customer_id, customer_name, customer_address, customer_country, customer_email, sku, qty, products.price[sku])
                
                if messy_data:
                    detailed_order = self.introduce_messy_data(detailed_order)
                    if random.random() < 0.05:
                        self.detailed_orders.append(detailed_order)

                self.detailed_orders.append(detailed_order)

                simple_order = (order_id, order_date, customer_id, sku, qty, products.price[sku])

                if messy_data:
                    simple_order = self.introduce_messy_data(simple_order)
                    if random.random() < 0.05:
                        self.simple_orders.append(simple_order)

                self.simple_orders.append(simple_order)

    def get_random_dates(self):
        
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

                for _ in range(day):
                    order_dates.append(current_date)
                
                current_date += timedelta(days=1)
                
        elif date_range == 0:

            order_dates = [current_date] * self.num_orders

        else: raise ValueError('Date cannot be in the future.') 

        return order_dates

    def introduce_messy_data(self,order):

        messy_data = list(order)
        
        if random.random() < 0.05:  
            if random.random() < 0.5:
                messy_data[1] = messy_data[1].strftime('%d/%m/%Y')
            else:
                messy_data[1] = messy_data[1].strftime('%d-%m-%Y')

        if random.random() < 0.1:
                idx = random.randint(0, len(order) - 4)
                messy_data[idx] = None

        if random.random() < 0.2:
                idx = random.randint(0, len(order) - 4)
                if type(messy_data[idx]) is str:
                    if random.random() < 0.5:
                        messy_data[idx] = messy_data[idx].lower()
                    else:
                        messy_data[idx] = messy_data[idx].upper()
                        
        return tuple(messy_data)

    def to_csv(self,detailed=True):

        date_today = datetime.today().date().strftime('%Y-%m-%d')
        file_path = f'Order_report_{date_today}.csv'

        with open(file_path, mode='w', newline='') as file:
            
            writer = csv.writer(file)
    
            if detailed:
                writer.writerow(['Order ID', 'Order Date', 'Customer ID', 'Name', 'Address', 'Country', 'Email', 'Item', 'Qty', 'Price'])
                for row in self.detailed_orders:
                    writer.writerow(row)
            else:
                writer.writerow(['Order ID', 'Order Date', 'Customer ID', 'Item', 'Qty', 'Price'])
                for row in self.simple_orders:
                    writer.writerow(row)


products = Products(label_prefix=LABEL_PREFIX, num_items=NUM_PRODUCTS, pricing=PRICING)

customers = Users(num_users=NUM_USERS, locales=LOCALES)

orders = Orders(num_orders=NUM_ORDERS, 
                users=customers, 
                products=products, 
                max_num_items=5, 
                start_date=ORDERS_START_DATE,
                messy_data=MESSY_DATA
                )

products.to_csv()
customers.to_csv()
orders.to_csv()
