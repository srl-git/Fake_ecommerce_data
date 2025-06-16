from datetime import datetime, timezone

from data_generator.Orders import Orders
from data_generator.Products import Products
from data_generator.Users import Users
from shared.db_connection import get_session
from shared.db_models import Order, OrdersModel, Product, ProductsModel, UsersModel


class Ecommerce:
    """
    A class to generate, retrieve, and export product and order/user data.

    Attributes:
        locales (list[str]): List of locale codes to be used for generating fake user data.

    """

    def __init__(self, locales: list[str]) -> None:
        self.products = Products()
        self.users = Users(locales)
        self.orders = Orders()

    def __str__(self) -> str:
        with get_session() as db:
            num_products = db.query(ProductsModel).count()
            num_users = db.query(UsersModel).count()
            num_orders = db.query(OrdersModel).count()

        return f"There are {num_products} products, {num_users} users, and {num_orders} orders in the database."

    def create_orders(self, num_orders: int, max_num_items: int) -> list[Order] | None:
        """
        Generates and adds fake orders to the database.

        Args:
            num_orders (int): Number of orders to generate.
            max_num_items (int): Maximum number of items in an order.

        Returns:
            list[Order] | None: A list of created Order dataclass instances.

        """
        return self.orders.create(
            users=self.users,
            num_orders=num_orders,
            max_num_items=max_num_items,
        )

    def create_products(
        self,
        label_prefix: str | list[str],
        preorder_weeks: int,
        num_items: int,
        pricing: list[float],
        creation_date: str | datetime = datetime.now(tz=timezone.utc),
    ) -> list[Product] | None:
        """
        Generates and adds fake products to the database.

        Args:
            label_prefix (str | list[str]): The prefix to use when creating the product catalogue number.
            preorder_weeks (int): Number of weeks between the creation_date and the product release date.
            num_items (int): Number of products to generate.
            pricing (list[float]): A list of prices to be randomly assigned to each product generated.
            creation_date (str | datetime): The date the products are added to the ecommerce store.

        Returns:
            list[Product] | None: A list of created Product dataclass instances.

        """
        if num_items < 1:
            return None

        return self.products.create(
            label_prefix=label_prefix,
            preorder_weeks=preorder_weeks,
            num_items=num_items,
            pricing=pricing,
            creation_date=creation_date,
        )

    def to_csv(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        *,
        messy_data: bool = False,
        local_file: bool = True,
        cloud_storage_file: bool = False,
    ) -> None:
        """
        Export product, user and order data to a CSV file locally and/or to Google Cloud Storage.

        Args:
            start_date (str | None): Start date (inclusive) in 'YYYY-MM-DD' format.
            end_date (str | None): End date (inclusive) in 'YYYY-MM-DD' format.
            messy_data (bool): If True, introduces a randomised amount of 'dirty' data to the order data.
            local_file (bool): If True, save the CSV file locally.
            cloud_storage_file (bool): If True, upload the CSV to a cloud storage bucket.

        """
        self.products.to_csv(
            start_date=start_date,
            end_date=end_date,
            local_file=local_file,
            cloud_storage_file=cloud_storage_file,
        )
        self.users.to_csv(
            start_date=start_date,
            end_date=end_date,
            local_file=local_file,
            cloud_storage_file=cloud_storage_file,
        )
        self.orders.to_csv(
            start_date=start_date,
            end_date=end_date,
            messy_data=messy_data,
            local_file=local_file,
            cloud_storage_file=cloud_storage_file,
        )
