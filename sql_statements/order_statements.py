create_order_table = '''
            CREATE TABLE IF NOT EXISTS Orders (
                order_line_id INTEGER PRIMARY KEY,
                order_id INTEGER,
                user_id TEXT,
                item_sku TEXT,
                qty INTEGER,
                item_price REAL,
                date_created DATETIME,
                FOREIGN KEY(user_id)
                    REFERENCES Users (user_id));
        '''

drop_order_table = '''
            DROP TABLE Orders;
        '''

add_orders_to_db = '''
            INSERT INTO Orders (
                order_id,
                user_id,
                item_sku,
                qty,
                item_price,
                date_created
            ) 
            VALUES (?, ?, ?, ?, ?, ?)
        '''

get_count_orders = '''
            SELECT COUNT(DISTINCT(order_id)) 
            FROM Orders
        '''

get_orders = '''
            SELECT * 
            FROM Orders
        '''

get_orders_by_date_range =  '''
            SELECT *
            FROM Orders
            WHERE DATE(date_created) BETWEEN ? AND ?;                   
        '''

get_last_order_id = '''
            SELECT order_id
            FROM Orders
            ORDER BY order_line_id DESC
            LIMIT 1
        '''