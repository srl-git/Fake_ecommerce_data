import numpy as np

ENTRIES = 10

# product sku, item qty, order qty

def generate_product_skus(label_prefix,count):
    product_skus = [f"{label_prefix}{i:03d}" for i in range(1,count)]
    return product_skus

def generate_order_qty(max_qty):
    order_qty = np.random.randint(1,max_qty)
    return order_qty

def generate_line_qty():
    range = np.arange(1, 6)
    weights = np.exp(-0.7 * (range - 1))
    probabilities = weights / weights.sum()   
    line_qty = int(np.random.choice(range,p=probabilities))
    return line_qty


def generate_order(max_num_items):
    items_in_order = generate_order_qty(max_num_items)
    random_product_list = []
    random_line_qty_list = []
    for i in range(items_in_order):
        random_product = str(np.random.choice(list_product_sku))
        if random_product in random_product_list:
            continue
        else: random_product_list.append(random_product)
        random_line_qty = generate_line_qty()
        random_line_qty_list.append(random_line_qty)
    order_entry = list(zip(random_product_list,random_line_qty_list))
    return order_entry

list_product_sku = generate_product_skus("FADE",30)
# print(list_product_sku)

# order_qty = generate_order_qty(5,ENTRIES)
# print(order_qty)

# line_qty = generate_line_qty(ENTRIES)
# print(line_qty)

order_example = generate_order(max_num_items=5)
print(order_example)
