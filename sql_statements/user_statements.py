create_user_table = '''
            CREATE TABLE IF NOT EXISTS Users (
                user_id INTEGER PRIMARY KEY,
                user_name TEXT,
                user_address TEXT,
                user_country TEXT,
                user_email TEXT,
                date_created DATETIME,
                date_updated DATETIME,
                user_popularity REAL);
        '''

drop_user_table = '''
            DROP TABLE Users;
        '''

add_users_to_db = '''
            INSERT INTO Users (
                user_name,
                user_address,
                user_country,
                user_email,
                date_created,
                date_updated,
                user_popularity
            ) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        '''

update_users = '''
            UPDATE Users 
            SET 
                user_name = ?,
                user_address = ?,
                user_country = ?,
                user_email = ?,
                date_updated = ?
            WHERE 
                user_id = ?
        '''

get_count_users = '''
            SELECT COUNT(*) 
            FROM Users
        '''

get_users = '''
            SELECT * 
            FROM Users
        '''

get_users_by_date_range = '''
            SELECT *
            FROM Users
            WHERE DATE(date_created) BETWEEN ? AND ?;                   
        '''

get_last_added = '''
            SELECT *
            FROM Users
            ORDER BY date_created DESC
            LIMIT 1
        '''

get_last_updated = '''
            SELECT *
            FROM Users
            ORDER BY date_updated DESC
            LIMIT 1
        '''

get_upper_limit = '''
            SELECT MAX(user_popularity) 
            FROM Users
        '''

get_popularity_scores = '''
            SELECT user_popularity 
            FROM Users
        '''

set_popularity_scores = '''
            UPDATE Users 
            SET user_popularity = ? 
            WHERE user_popularity = ?
        '''

def get_users_by_id(user_id):

    placeholder = ', '.join(['?'] * len(user_id))
    statement = f'''
            SELECT * 
            FROM Users 
            WHERE user_id IN ({placeholder})
        '''
    return statement