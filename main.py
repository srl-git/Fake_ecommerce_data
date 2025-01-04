from faker import Faker
import csv
import random

LABEL_PREFIX = 'DARE'
NUM_PRODUCTS = 100

NUM_USERS = 45000
LOCALES = ['en_GB','en_US','fr_FR','en_CA','de_DE']

NUM_ORDERS = 50000


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
            address = profile['address']
            country = fake.current_country()
            email = f'{name.lower().replace(' ','').replace('.','')}@example.com'
            user = (user_id, name, address, country, email)
            self.users.append(user)

    def to_csv(self,file_path='customer_data.csv'):

        self.file_path = file_path

        with open(self.file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            
            writer.writerow(['User ID', 'Name', 'Address', 'Country', 'Email'])
            
            for row in self.users:
                writer.writerow(row)            

class Orders:

    def __init__(self,num_orders,users,products,max_num_items):
        
        self.num_orders = num_orders
        self.users = users
        self.products = products
        self.max_num_items = max_num_items

        self.detailed_orders = []
        self.simple_orders = []

        for i in range(self.num_orders):

            items_in_order = random.randint(1,self.max_num_items)

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

                detailed_order = (order_id, customer_id, customer_name, customer_address, customer_country, customer_email, sku, qty, products.price[sku])
                self.detailed_orders.append(detailed_order)

                simple_order = (order_id, customer_id, sku, qty, products.price[sku])
                self.simple_orders.append(simple_order)


    def to_csv(self,detailed=True,file_path = 'orders.csv'):

        self.file_path = file_path

        with open(self.file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            
            if detailed:
                writer.writerow(['Order ID', 'Customer ID', 'Name', 'Address', 'Country', 'Email', 'Item', 'Qty', 'Price'])
                
                for row in self.detailed_orders:
                    writer.writerow(row)
            
            else:
                writer.writerow(['Order ID', 'Customer ID', 'Item', 'Qty', 'Price'])
                
                for row in self.simple_orders:
                    writer.writerow(row)

products = Products(label_prefix=LABEL_PREFIX, entries=NUM_PRODUCTS)
customers = Users(num_users=NUM_USERS, locales=LOCALES)
orders = Orders(num_orders=NUM_ORDERS, users=customers, products=products, max_num_items=5)

products.to_csv()
customers.to_csv()
orders.to_csv()