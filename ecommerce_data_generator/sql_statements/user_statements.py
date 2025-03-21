create_user_table = '''
            CREATE TABLE IF NOT EXISTS Users (
                user_id INTEGER PRIMARY KEY,
                user_name TEXT,
                user_address TEXT,
                user_country TEXT,
                user_email TEXT,
                date_created TIMESTAMP);
        '''

drop_user_table = '''
            DROP TABLE IF EXISTS Users;
        '''

add_users_to_db = '''
            INSERT INTO Users (
                user_id,
                user_name,
                user_address,
                user_country,
                user_email,
                date_created
            ) 
            VALUES (%s, %s, %s, %s, %s, %s);
        '''

get_count_users = '''
            SELECT COUNT(*) 
            FROM Users;
        '''

get_users = '''
            SELECT * 
            FROM Users;
        '''

get_users_by_date_range = '''
            SELECT *
            FROM Users
            WHERE date_created::DATE BETWEEN %s AND %s;                   
        '''

get_random_users = '''
            SELECT *
            FROM Users
            ORDER BY RANDOM()
            LIMIT %s;
        '''

get_last_user_id = '''
            SELECT user_id
            FROM Users
            ORDER BY user_id DESC
            LIMIT 1;    
        '''

def get_users_by_id(user_id):

    placeholder = ', '.join(['%s'] * len(user_id))
    statement = f'''
            SELECT * 
            FROM Users 
            WHERE user_id IN ({placeholder})
        '''
    return statement