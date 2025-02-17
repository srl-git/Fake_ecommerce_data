from datetime import datetime, timedelta
import random
import csv
import io

from google_cloud import CloudSQLConnection, upload_to_bucket
import sql_statements as sql

class Products:
    
    def __init__(self) -> None:
        
        self._initialise_db_table()

    def __repr__(self) -> str:
        
        return f'Products()'

    def __str__(self) -> str:
       
        return f'There are {self.get_count_products()} products in the database'
     
    def create(
        self, 
        label_prefix: str, 
        num_items: int, 
        pricing: list[float],
        creation_date: str | datetime = datetime.now()
    ) -> None:
        
        if num_items == 0:
            return
        
        self._validate_create_args(label_prefix, num_items, pricing, creation_date)
        
        if not isinstance(creation_date, datetime):
            creation_date = datetime.strptime(creation_date, '%Y-%m-%d')

        release_date = (creation_date + timedelta(weeks=6)).strftime('%Y-%m-%d')
        date_created = date_updated = creation_date.strftime('%Y-%m-%d')
        popularity_upper_limit = self._get_upper_limit()
        index = self._get_sku_index(label_prefix)
        
        products = []
        
        for i in range(num_items):

            item_sku = f'{label_prefix}{index + 1 + i :03}'
            item_price = random.choice(pricing)
            weighting = random.uniform(0.0, popularity_upper_limit)
            is_active = True
            product = (item_sku, item_price, release_date, date_created, date_updated, is_active, weighting)
            products.append(product)

        self._add_to_db(products)
    
    def update(
        self,
        item_sku: str | list[str],
        item_price: int | float | list[int | float] | None = None,
        is_active: bool | list[bool] | None = None
    ) -> None:
    
        self._validate_update_args(item_sku, item_price, is_active)

        products_to_update = self.get_products(item_sku)
        
        if item_price is not None:
            if isinstance(item_price, list) and len(item_price) == 1:
                item_price = item_price[0]
            if isinstance(item_price, (int, float)):
                item_price = [item_price] * len(products_to_update)
                
        if is_active is not None:
            if isinstance(is_active, list) and len(is_active) == 1:
                is_active = is_active[0]
            if isinstance(is_active, bool):
                is_active = [is_active] * len(products_to_update)

        update_data = []

        if products_to_update:
            for product in products_to_update:

                sku_to_update = product[0]
                index = item_sku.index(sku_to_update)
                price_updated = item_price[index] if item_price is not None else product[1]
                date_updated = datetime.today().strftime('%Y-%m-%d')
                is_active_updated = is_active[index] if is_active is not None else product[5]
                update_data.append((price_updated, date_updated, is_active_updated, sku_to_update))

            with CloudSQLConnection() as db:
                db.cursor.executemany(sql.product_statements.update_products,(update_data))  

    def get_count_products(self) -> int:
        
        with CloudSQLConnection() as db:
            db.cursor.execute(sql.product_statements.get_count_products)
            count_products = db.cursor.fetchone()[0]
            
        return count_products

    def get_products(
        self,
        item_sku: str | list[str] | tuple[str] | None = None
    ) -> list[tuple]:
        
        with CloudSQLConnection() as db:

            if item_sku:
                if not (isinstance(item_sku, (str, list, tuple)) and all(isinstance(s, str) for s in item_sku)):
                    raise ValueError(
                'ERROR in Products.get_products(). Expected a non empty string or list of strings for item_sku argument. '
                f'Receieved value "{item_sku}" of type {type(item_sku).__name__}.'
                )
                if isinstance(item_sku, str):
                    item_sku = (item_sku, )
                db.cursor.execute(sql.product_statements.get_products_by_sku(item_sku), item_sku)
                products = db.cursor.fetchall()
            
            else:
                db.cursor.execute(sql.product_statements.get_products)
                products = db.cursor.fetchall()

        return products
    
    def get_products_by_date_range(
        self,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None
    ) -> list[tuple]:

        start_date = '0000-01-01' if start_date is None else start_date
        end_date = datetime.today() if end_date is None else end_date

        if isinstance(start_date, datetime):
            start_date = start_date.strftime('%Y-%m-%d')
        if isinstance(end_date, datetime):
            end_date = end_date.strftime('%Y-%m-%d')

        try:
            datetime.fromisoformat(start_date)
            datetime.fromisoformat(end_date)
        except ValueError:
            raise ValueError(
                'ERROR in Products.get_products_by_date_range()). '
                'Expected a datetime object or a valid date string in format YYYY-MM-DD for start_date and end_date arguments.'
            )

        with CloudSQLConnection() as db:
            db.cursor.execute(sql.product_statements.get_products_by_date_range,(start_date, end_date))
            products_by_date_range = db.cursor.fetchall()
            
        return products_by_date_range
    
    def to_csv(
        self,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        local_file: bool = True,
        cloud_storage_file: bool = False
    ) -> None:

        if start_date or end_date:
            try:
                export_data = [product[:-1] for product in self.get_products_by_date_range(start_date,end_date)]
                
                if isinstance(start_date, datetime):
                    start_date = start_date.strftime('%Y-%m-%d')
                if isinstance(end_date, datetime):
                    end_date = end_date.strftime('%Y-%m-%d')

                start_date = start_date if start_date else ''
                end_date = f'_{end_date}' if end_date else ''                    
                file_path = f'Product_report_{start_date}{end_date}.csv'

            except ValueError:
                raise ValueError(
                'ERROR in Products.to_csv(). '
                'Expected a datetime object or a valid date string in format YYYY-MM-DD for start_date and end_date arguments.'
                )
        else:
            export_data = [product[:-1] for product in self.get_products()]
            date_today = datetime.today().strftime('%Y-%m-%d')
            file_path = f'Product_report_{date_today}.csv'

        if len(export_data) == 0:
            return
        if local_file:
            self._save_to_file(export_data, file_path)
        if cloud_storage_file:
            self._save_to_cloud_storage(export_data, file_path)
    
    def _save_to_file(self, export_data: list[tuple], file_path: str):

        with open(file_path, mode='w', newline='') as file:
            
            writer = csv.writer(file)
            writer.writerow(['Product SKU', 'Price', 'Release Date', 'Date Created', 'Date Updated', 'Active'])
            
            for row in export_data:
                writer.writerow(row)
                
    def _save_to_cloud_storage(self, export_data: list[tuple], file_path: str):

        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(['Product SKU', 'Price', 'Release Date', 'Date Created', 'Date Updated', 'Active'])

        for row in export_data:
            writer.writerow(row)
        
        upload_data = csv_buffer.getvalue()

        upload_to_bucket(f'product_reports/{file_path}', upload_data, 'srl_ecommerce')
        

    def _initialise_db_table(self) -> None:
        
        with CloudSQLConnection() as db:
            db.cursor.execute(sql.product_statements.create_product_table)

    def _drop_db_table(self) -> None:

        with CloudSQLConnection() as db:
            db.cursor.execute(sql.product_statements.drop_product_table)

    def _validate_create_args(
        self, 
        label_prefix: str, 
        num_items: int, 
        pricing: list[float],
        creation_date: str | datetime
    ) -> None:

        if not (isinstance(label_prefix, str) and label_prefix.strip()):
            raise ValueError(
                'ERROR in Products.create(). Expected a non empty string value for label_prefix argument. '
                f'Received value: "{label_prefix}" of type: {type(label_prefix).__name__}.'
            )
        
        if not isinstance(num_items, int):
            raise ValueError(
                'ERROR in Products.create(). Expected a positive integer value for num_items argument. '
                f'Received value: "{num_items}" of type: {type(num_items).__name__}.'
            )

        if not isinstance(pricing, list):
            raise TypeError(
                'ERROR in Products.create(). Expected a list of positive floats or integers for pricing argument. '
                f'Received value: "{pricing}" of type {type(pricing).__name__}.'
            )
        
        if not all(isinstance(val, (int, float)) and val > 0 for val in pricing):
            raise ValueError(
                'ERROR in Products.create(). Invalid value in pricing argument: '
                'All elements in the list must be positive integers or floats.'
            )
        
        try:
            if not isinstance(creation_date, datetime):
                datetime.fromisoformat(creation_date)
        except ValueError:
            raise ValueError(
                'ERROR in Products.create(). '
                'Expected a datetime object or valid date string in format YYYY-MM-DD for creation_date argument.'
            )

    def _validate_update_args(
        self, 
        item_sku: str | list[str],
        item_price: int | float | list[int | float] | None,
        is_active: bool | list[bool] | None) -> None:
        
        if not(item_sku and isinstance(item_sku, (list, str)) and all(isinstance(s, str) for s in item_sku)):
            raise ValueError(
                'ERROR in Products.update(). Expected a non empty string or list of strings for item_sku argument. '
                f'Receieved value "{item_sku}" of type {type(item_sku).__name__}.'
            )
        
        invalid_skus = None

        if isinstance(item_sku, list):
            invalid_skus = [sku for sku in item_sku if sku not in set(product[0] for product in self.get_products(item_sku))]
       
        if isinstance(item_sku, str):
            invalid_skus = item_sku if item_sku not in set(product[0] for product in self.get_products(item_sku)) else None
        
        if invalid_skus:
            raise ValueError(
                'ERROR in Products.update(). The following item_skus do not exist in the database:\n'
                f'{invalid_skus}'
            )
        
        if item_price is not None:

            if not isinstance(item_price, (list, int, float)) or isinstance(item_price, bool):
                raise TypeError(
                    'ERROR in Products.update(). '
                    'Expected a positive integer, float, or a list of positive integers/floats for item_price argument. '
                    f'Received value "{item_price}" of type: {type(item_price).__name__}.'
                )
            
            if isinstance(item_price, list):
                
                if isinstance(item_sku,str) and len(item_price) != 1:
                    raise ValueError(
                        'ERROR in Products.update(). Mismatch between item_price and item_sku: '
                        f'Expected 1 price but got {len(item_price)}.'
                    )
                
                if len(item_price) != len(item_sku):
                    raise ValueError(
                        'ERROR in Products.update(). Mismatch between item_price and item_sku: '
                        f'Expected either 1 positive integer/float or a list of {len(item_sku)} integers/floats for item_price argument.'
                    )
                
                if not all(isinstance(element, (int, float)) and element >= 0 for element in item_price):
                    raise ValueError(
                        'ERROR in Products.update(). '
                        'Invalid value in item_price argument: All elements in the list must be positive integers or floats.'
                    )

            if isinstance(item_price, (int, float)):
                if item_price < 0:
                    raise ValueError(
                        'ERROR in Products.update(). '
                        'Expected a positive integer, float, or a list of positive integers/floats for item_price argument. '
                    )
                
            if is_active is not None:

                if not isinstance(is_active, (list, bool)):
                    raise TypeError(
                        'ERROR in Products.update(). Expected a boolean or list of booleans for is_active argument. '
                        f'Received value: "{is_active}" of type: {type(is_active).__name__}.'
                    )
                
                if isinstance(is_active, list):
                    
                    if isinstance(item_sku, str):
                        raise ValueError(
                            'ERROR in Products.update(). Mismatch between is_active and item_sku: '
                            f'Expected 1 boolean value but got {len(is_active)}.'
                        )
                    
                    if len(is_active) not in (1, len(item_sku)):
                        raise ValueError(
                            'ERROR in Products.update(). Mismatch between is_active and item_sku: '
                            f'Expected either 1 or {len(item_sku)} values, but got {len(is_active)}.'
                        )
                    
                    if not all(isinstance(element, bool) for element in is_active):
                        raise ValueError(
                            'ERROR in Products.update(). '
                            'Invalid value in is_active argument: All elements in the list must be booleans.'
                        )

    def _get_sku_index(self, label_prefix: str) -> int:
        
        with CloudSQLConnection() as db:
            db.cursor.execute(sql.product_statements.get_sku_index, (f'{label_prefix}%',))
            sku_index = db.cursor.fetchone()[0]

        return sku_index

    def _get_upper_limit(self) -> float:

        with CloudSQLConnection() as db:
            db.cursor.execute(sql.product_statements.get_upper_limit)
            max_popularity_score = db.cursor.fetchone()[0]

        popularity_upper_limit = max_popularity_score * 1.5 if max_popularity_score else 1

        return popularity_upper_limit

    def _set_popularity_scores(self) -> None:
 
        with CloudSQLConnection() as db:
            db.cursor.execute(sql.product_statements.get_popularity_scores)
            old_popularity_scores = tuple(score[0] for score in db.cursor.fetchall())

        normalizer = 1 / float(sum(old_popularity_scores))
        new_popularity_scores = tuple(score * normalizer for score in old_popularity_scores)
        update_scores = zip(new_popularity_scores, old_popularity_scores)

        with CloudSQLConnection() as db:
            db.cursor.executemany(sql.product_statements.set_popularity_scores, (update_scores))

    def _add_to_db(
        self,
        products: list[tuple]
    ) -> None:
            
        with CloudSQLConnection() as db:
            db.cursor.executemany(sql.product_statements.add_products_to_db, products)
        
        self._set_popularity_scores()
