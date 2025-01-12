create_user_table = '''
            CREATE TABLE IF NOT EXISTS Users (
                user_id INT PRIMARY KEY,
                user_name TEXT,
                user_address TEXT,
                user_country TEXT,
                user_email TEXT,
                date_created DATETIME,
                date_updated DATETIME);
        '''