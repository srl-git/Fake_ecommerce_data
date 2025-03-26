from datetime import date

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from google_cloud import CloudSQLConnection
import logger

app = FastAPI()

log = logger.get_logger(__name__)

class ProductQuery(BaseModel):
    
    date_updated: date


@app.get('/products')
def get_products(date_updated: date | None = None) -> dict | None:
    where_clause = 'WHERE DATE(date_updated) = %s' if date_updated else ''
    sql_query = f'''
        SELECT 
            json_agg(t)
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
    try:
        with CloudSQLConnection() as db:
            if date_updated:
                db.cursor.execute(sql_query,(date_updated,))
            else:
                db.cursor.execute(sql_query)
            result = db.cursor.fetchone()
    except Exception as e:
        print(f'Database error: {e}')
        raise HTTPException(status_code=500, detail='Internal server error. Please try again later.')
                  
    if result and result[0]:
        return result[0]
    else:
        raise HTTPException(status_code=404, detail=f'No products found matching date_updated={date_updated}.')
