create_product_table = '''
            CREATE TABLE IF NOT EXISTS Products (
                item_sku TEXT PRIMARY KEY,
                item_price REAL,
                date_created DATETIME,
                date_updated DATETIME,
                active BOOLEAN,
                item_popularity REAL);
        '''

drop_product_table = '''
            DROP TABLE Products;
        '''

get_sku_index = '''
            SELECT COUNT(item_sku) 
            FROM Products 
            WHERE item_sku LIKE ?
        '''

get_upper_limit = '''
            SELECT MAX(item_popularity) 
            FROM Products
        '''

get_popularity_scores = '''
            SELECT item_popularity 
            FROM Products
        '''

set_popularity_scores = '''
            UPDATE Products 
            SET item_popularity = ? 
            WHERE item_popularity = ?
        '''

add_products_to_db = '''
            INSERT INTO Products (
                item_sku, 
                item_price, 
                date_created, 
                date_updated, 
                active, 
                item_popularity
            ) 
            VALUES (?, ?, ?, ?, ?, ?)
        '''

update_products = '''
            UPDATE Products 
            SET 
                item_price = ?,
                date_updated = ?,
                active = ? 
            WHERE 
                item_sku = ?
        '''

get_products = '''
            SELECT * 
            FROM Products
        '''

get_products_by_date_range = '''
            SELECT *
            FROM Products
            WHERE DATE(date_created) BETWEEN ? AND ?;                   
        '''

get_last_added = '''
            SELECT *
            FROM Products
            ORDER BY rowid DESC
            LIMIT 1
        '''

get_last_updated = '''
            SELECT *
            FROM Products
            ORDER BY date_updated DESC
            LIMIT 1
        '''

def get_products_by_sku(sku):

    placeholder = ', '.join(['?'] * len(sku))
    statement = f'''
            SELECT * 
            FROM Products 
            WHERE item_sku IN ({placeholder})
        '''
    return statement

