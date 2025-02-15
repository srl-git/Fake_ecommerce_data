from datetime import datetime

from ecommerce.Products import Products
from ecommerce.Users import Users
from ecommerce.Orders import Orders

class Ecommerce:
    
    def __init__(self, db_path: str) -> None:
        
        if not (isinstance(db_path, str) and db_path.endswith('.db')):
            raise ValueError('The path to the database file must be a non empty string with a .db file extension')
        
        self.db_path = db_path

        self.products = Products(self.db_path)
        self.users = Users(self.db_path)
        self.orders = Orders(self.db_path)

    def __repr__(self) -> str:
        
        return f'Ecommerce({self.db_path})'

    def __str__(self) -> str:
        
        num_products = self.products.get_count_products()
        num_users = self.users.get_count_users()
        num_orders = self.orders.get_count_orders()

        return f'There are {num_products} products, {num_users} users, and {num_orders} orders in the database'
    
    def create_orders_and_users(
        self, 
        locales: list[str], 
        num_orders: int,
        max_num_items: int
    ) -> None:  

        self.orders.create(self.users, locales, self.products, num_orders, max_num_items)

    def to_csv(
        self,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        messy_data: bool = False,
        local_file: bool = True,
        cloud_storage_file: bool = False
    ) -> None:
                
        self.products.to_csv(start_date, end_date, local_file, cloud_storage_file)
        self.users.to_csv(start_date, end_date, local_file, cloud_storage_file)
        self.orders.to_csv(start_date, end_date, messy_data, local_file, cloud_storage_file)
