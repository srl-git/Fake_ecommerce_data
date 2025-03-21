create_order_table = '''
            CREATE TABLE IF NOT EXISTS Orders (
                order_line_id SERIAL PRIMARY KEY,
                order_id INTEGER,
                user_id INTEGER,
                item_sku TEXT,
                qty INTEGER,
                item_price DECIMAL(12,2),
                date_created TIMESTAMP,
                FOREIGN KEY(user_id)
                    REFERENCES Users (user_id));
        '''

drop_order_table = '''
            DROP TABLE IF EXISTS Orders;
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
            VALUES (%s, %s, %s, %s, %s, %s);
        '''

get_count_orders = '''
            SELECT COUNT(DISTINCT(order_id)) 
            FROM Orders;
        '''

get_orders = '''
            SELECT * 
            FROM Orders;
        '''

get_orders_by_date_range =  '''
            SELECT *
            FROM Orders
            WHERE DATE(date_created) BETWEEN %s AND %s;                   
        '''

get_last_order_id = '''
            SELECT order_id
            FROM Orders
            ORDER BY order_line_id DESC
            LIMIT 1;
        '''