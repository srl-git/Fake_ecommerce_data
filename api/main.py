from fastapi import FastAPI
from datetime import datetime
from google_cloud import CloudSQLConnection

app = FastAPI()


@app.get('/products')
def get_products(date_updated: str=''):

    if date_updated:
        try:
            datetime.fromisoformat(date_updated)
        except:
            return 'Invalid date string, expected date in format YYYY-mm-dd'
    
    where_clause = 'WHERE DATE(date_updated) = %s' if date_updated else ''
    query = f'''
        SELECT json_agg(t)
        FROM (
            SELECT
                item_sku,
                item_price,
                release_date,
                date_created,
                date_updated,
                active
            FROM Products
            {where_clause}
        ) t;
        '''
    
    with CloudSQLConnection() as db:
        db.cursor.execute(query,(date_updated,)) if date_updated else db.cursor.execute(query)
        result = db.cursor.fetchone()[0]
    return result
    
