import numpy as np
import csv

NUM_PRODUCTS = 35
NUM_ORDERS = 10000

# product sku, product price, product popularity, product name
# order ID, items in order, qty items in order, username, user email, address
# username, user email


class Products:

    def __init__(self,label_prefix,entries):
        self.label_prefix = label_prefix
        self.entries = entries
        self.skus = [f"{self.label_prefix}{i:03d}" for i in range(1,self.entries + 1)]
        weighting = np.random.rand(self.entries)
        self.popularity = weighting / weighting.sum()
        self.prices = [int(np.random.choice([18,19,20,21])) for i in range(0,self.entries)]
        self.price = {sku: price for sku, price in zip(self.skus, self.prices)}


def generate_num_items(max_num_items):
    num_items = np.random.randint(1,max_num_items)
    return num_items

def generate_orders(num_orders,products,max_num_items):
    
    orders = []

    for i in range(num_orders):

        order_id = f'{(i + 1):03}'

        items_in_order = generate_num_items(max_num_items)

        order = {}

        for i in range(items_in_order):

            random_product = str(np.random.choice(products.skus,p=products.popularity))
            
            if random_product in order:
                order[random_product] += 1
            else:
                order[random_product] = 1
        
        orders.append(order)

    return orders


warp_products = Products("WARP",NUM_PRODUCTS)
all_products = [(warp_products.skus[i],warp_products.prices[i],warp_products.popularity[i]) for i in range(warp_products.entries)]
# print(all_products)


# file_path = 'product_data.csv'

# with open(file_path, mode='w', newline='') as file:
#     writer = csv.writer(file)
    
#     writer.writerow(['SKU', 'Price', 'Popularity'])
    
#     for row in all_products:
#         writer.writerow(row)


# print(products.skus)
# print(products.popularity)


orders = generate_orders(num_orders=NUM_ORDERS,products=warp_products,max_num_items=5)
# print(orders)


totals = {}

for d in orders:
    for key, value in d.items():
        totals[key] = totals.get(key, 0) + value

print(totals)

print(warp_products.price["WARP001"])