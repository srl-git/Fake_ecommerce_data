create_product_table = '''
            CREATE TABLE IF NOT EXISTS Products (
                item_sku TEXT PRIMARY KEY,
                item_price DECIMAL(12,2),
                release_date TIMESTAMP,
                date_created TIMESTAMP,
                date_updated TIMESTAMP,
                active BOOLEAN,
                item_popularity DOUBLE PRECISION);
        '''

drop_product_table = '''
            DROP TABLE IF EXISTS Products;
        '''

get_sku_index = '''
            SELECT COUNT(item_sku) 
            FROM Products 
            WHERE item_sku LIKE %s;
        '''

get_upper_limit = '''
            SELECT MAX(item_popularity) 
            FROM Products;
        '''

get_popularity_scores = '''
            SELECT item_popularity 
            FROM Products;
        '''

set_popularity_scores = '''
            UPDATE Products 
            SET item_popularity = %s 
            WHERE item_popularity = %s;
        '''

add_products_to_db = '''
            INSERT INTO Products (
                item_sku, 
                item_price,
                release_date, 
                date_created, 
                date_updated, 
                active, 
                item_popularity
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        '''

update_products = '''
            UPDATE Products 
            SET 
                item_price = %s,
                date_updated = %s,
                active = %s 
            WHERE 
                item_sku = %s;
        '''

get_count_products = '''
            SELECT COUNT(*) 
            FROM Products;
        '''

get_products = '''
            SELECT * 
            FROM Products;
        '''

get_products_by_date_range = '''
            SELECT *
            FROM Products
            WHERE date_created::DATE BETWEEN %s AND %s;                   
        '''

def get_products_by_sku(sku):

    placeholder = ', '.join(['%s'] * len(sku))
    statement = f'''
            SELECT * 
            FROM Products 
            WHERE item_sku IN ({placeholder})
        '''
    return statement

