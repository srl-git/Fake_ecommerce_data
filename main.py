from faker import Faker
import csv
import random
from unidecode import unidecode

LABEL_PREFIX = 'STDR'
NUM_PRODUCTS = 30

NUM_USERS = 10
LOCALES = ['en_GB','en_US','fr_FR','en_CA','de_DE']

NUM_ORDERS = 200
ORDERS_START_DATE = '-2y'
MESSY_DATA = False


class Products:

    def __init__(self,label_prefix,entries):

        self.label_prefix = label_prefix
        self.entries = entries
        self.skus = [f'{self.label_prefix}{i:03}' for i in range(1,self.entries + 1)]
        weighting = [random.random() for _ in range(self.entries)]
        self.popularity = [w / sum(weighting) for w in weighting]
        self.prices = [random.choice([18,19,20,21]) for i in range(0,self.entries)]
        self.price = {sku: price for sku, price in zip(self.skus, self.prices)}
        
        self.products = []

        for i in range(entries):
            product = (self.skus[i], self.prices[i])
            self.products.append(product)

    def to_csv(self,file_path='product_data.csv'):

        self.file_path = file_path

        with open(self.file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            
            writer.writerow(['Product SKU', 'Price'])
            
            for row in self.products:
                writer.writerow(row)

class Users:

    def __init__(self,num_users,locales):
        
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
        return f'{unidecode(name.lower().replace(' ','').replace('.',''))}@example.com'
    
    def to_csv(self,file_path='customer_data.csv'):

        self.file_path = file_path

        with open(self.file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            
            writer.writerow(['User ID', 'Name', 'Address', 'Country', 'Email'])
            
            for row in self.users:
                writer.writerow(row)            

class Orders:

    def __init__(self,num_orders,users,products,max_num_items,start_date,end_date='today',messy_data=False):
        
        self.num_orders = num_orders
        self.users = users
        self.products = products
        self.max_num_items = max_num_items
        self.start_date = start_date
        self.end_date = end_date

        self.detailed_orders = []
        self.simple_orders = []

        for i in range(self.num_orders):

            items_in_order = random.randint(1,self.max_num_items)

            fake = Faker()
            random_date = fake.date_between(start_date=self.start_date,end_date=self.end_date)
            order_date = random_date

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

            for j in range(items_in_order):

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

    def introduce_messy_data(self,order):

        messy_data = order
        
        if random.random() < 0.1:
                
                idx = random.randint(0, len(order) - 4)
                messy_data = list(order)
                messy_data[idx] = None

        if random.random() < 0.2:
                
                idx = random.randint(0, len(order) - 4)
                messy_data = list(order)

                if type(messy_data[idx]) is str:
                    
                    if random.random() < 0.5:

                        messy_data[idx] = messy_data[idx].lower()
                        
                    else:
                        
                        messy_data[idx] = messy_data[idx].upper()
                        

        return tuple(messy_data)

    def to_csv(self,detailed=True,file_path = 'orders.csv'):

        self.file_path = file_path

        with open(self.file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            
            if detailed:
                writer.writerow(['Order ID', 'Order Date', 'Customer ID', 'Name', 'Address', 'Country', 'Email', 'Item', 'Qty', 'Price'])
                
                for row in self.detailed_orders:
                    writer.writerow(row)
            
            else:
                writer.writerow(['Order ID', 'Order Date', 'Customer ID', 'Item', 'Qty', 'Price'])
                
                for row in self.simple_orders:
                    writer.writerow(row)

products = Products(label_prefix=LABEL_PREFIX, entries=NUM_PRODUCTS)
customers = Users(num_users=NUM_USERS, locales=LOCALES)
orders = Orders(num_orders=NUM_ORDERS, users=customers, products=products, max_num_items=5, start_date=ORDERS_START_DATE,messy_data=MESSY_DATA)

products.to_csv()
customers.to_csv()
orders.to_csv()
