import numpy as np

ENTRIES = 10

# product sku, item qty, order qty, username, user email

class Products:

    def __init__(self,label_prefix,entries):
        self.label_prefix = label_prefix
        self.entries = entries
        self.skus = [f"{self.label_prefix}{i:03d}" for i in range(1,self.entries + 1)]
        self.weighting = [round(np.random.rand(),2) for i in range(0,self.entries)]
        self.prices = [int(np.random.choice([18,19,20,21])) for i in range(0,self.entries)]


def generate_num_items(max_num_items):
    num_items = np.random.randint(1,max_num_items)
    return num_items

def generate_item_qty():
    range = np.arange(1, 6)
    weights = np.exp(-0.7 * (range - 1))
    probabilities = weights / weights.sum()   
    line_qty = int(np.random.choice(range,p=probabilities))
    return line_qty

# def generate_order(product_skus,max_num_items):
#     items_in_order = generate_num_items(max_num_items)
#     random_product_list = []
#     random_item_qty_list = []

#     for i in range(items_in_order):

#         random_product = str(np.random.choice(product_skus))
        
#         if random_product in random_product_list:
#             continue
#         else: 
#             random_product_list.append(random_product)

#         random_item_qty = generate_item_qty()
#         random_item_qty_list.append(random_item_qty)

#     order = list(zip(random_product_list,random_item_qty_list))
#     return order

def generate_order(products,max_num_items):
    
    items_in_order = generate_num_items(max_num_items)
    random_product_list = []
    random_item_qty_list = []

    weights = np.array(products.weighting)
    probabilities = weights / weights.sum()

    for i in range(items_in_order):

        random_product = str(np.random.choice(products.skus,p=probabilities))
        
        if random_product in random_product_list:
            continue
        else: 
            random_product_list.append(random_product)

        random_item_qty = generate_item_qty()
        random_item_qty_list.append(random_item_qty)

    order = list(zip(random_product_list,random_item_qty_list))
    return order


products = Products("WARP",ENTRIES)
all_products = [(products.skus[i],products.prices[i],products.weighting[i]) for i in range(products.entries)]

for i in range(30):

    order = generate_order(products,max_num_items=5)
    print(order)
