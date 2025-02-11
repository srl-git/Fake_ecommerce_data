from database.DatabaseConnection import DatabaseConnection
from ecommerce.Products import Products
from ecommerce.Users import Users
from ecommerce.Orders import Orders

class Ecommerce:
    
    def __init__(self, db_path: str) -> None:
        
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
    
    