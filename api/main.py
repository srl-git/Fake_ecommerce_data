from fastapi import FastAPI
from google_cloud import CloudSQLConnection

app = FastAPI()


@app.get('/products')
def get_products(date: str):
    with CloudSQLConnection() as db:
        query = '''
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
                WHERE DATE(date_updated) = %s
            ) t;
        '''
        db.cursor.execute(query,(date,))
        result = db.cursor.fetchall()
    return result
