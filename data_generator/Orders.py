import csv
import io
import math
import random
from dataclasses import astuple
from datetime import datetime, timezone

from sqlalchemy import func

from data_generator import Users
from data_generator.google_cloud_storage import upload_to_bucket
from shared.config import get_config
from shared.db_connection import get_session
from shared.db_models import Order, OrdersModel, ProductsModel, UsersModel
from shared.logger import get_logger

log = get_logger(__name__)
config = get_config()


class Orders:
    """A class to generate, retrieve, and export order data using SQLAlchemy."""

    def create(
        self,
        users: Users,
        num_orders: int,
        max_num_items: int,
        date_created: datetime,
    ) -> list[Order] | None:
        """
        Generates and adds fake orders to the database.

        Args:
            users (Users): An instance of the Users class.
            num_orders (int): Number of orders to generate.
            max_num_items (int): Maximum number of items in an order.
            date_created (datetime): The date the order was created.

        Returns:
            list[Order] | None: A list of created Order dataclass instances.

        """
        if num_orders == 0:
            log.debug("Skipping generating orders, num_orders= %s", num_orders)

            return None

        log.debug("Generating %s orders.", num_orders)

        products = self._get_active_products()
        user_ids = self._get_user_ids(users, num_orders, date_created)
        order_models = self._generate_order_lines(
            products,
            user_ids,
            num_orders,
            max_num_items,
            date_created,
        )

        with get_session() as db:
            db.add_all(order_models)
            db.flush()
            log.debug("Flushed %s orders to the database.", len(order_models))
            orders = [order.to_plain() for order in order_models]

        log.info("%s order line(s) added to the database.", len(orders))

        return orders

    def _get_active_products(self) -> dict[str, dict]:
        """
        Retrieve active products from the database and returns basic details.

        Raises:
            ValueError: If no products in the database, no orders can be created.

        Returns:
            dict: A dictionary of item_sku keys and item_price and item_popularity values {item_sku: {item_price: value}, {item_popularity: value}}

        """
        with get_session() as db:
            product_models = db.query(ProductsModel).all()
            if len(product_models) < 1:
                error_msg = "ERROR in Orders.create(): Can't generate orders without any products in the database."
                raise ValueError(error_msg)
            products = [p.to_plain() for p in product_models]

        active_products = {
            product.item_sku: {
                "item_price": product.item_price,
                "item_popularity": product.item_popularity,
            }
            for product in products
            if product.active
        }
        log.debug(
            "%d active products will be used for order generation.",
            len(active_products),
        )
        return active_products

    def _get_user_ids(self, users: Users, num_orders: int, date_created: datetime) -> list[int]:
        """
        Generate a list of new and existing user IDs for orders, weighted towards new users to simulate realistics user activity.

        Args:
            users (Users): An instance of the Users class.
            num_orders (int): The number of orders to generate.
            date_created (datetime): The date the user was created.

        Returns:
            list[int]: A list of unique user_ids.

        """
        ratio_previous_users = random.uniform(0.0, 0.1)
        num_previous_users = round(num_orders * ratio_previous_users)
        with get_session() as db:
            previous_users = db.query(UsersModel).order_by(func.random()).limit(num_previous_users).all()
            previous_users_ids = [user.user_id for user in previous_users]

        num_new_users = num_orders - len(previous_users_ids)
        new_users = users.create(num_new_users, date_created)
        new_users_ids = [user.user_id for user in new_users]
        all_users_ids = previous_users_ids + new_users_ids
        random.shuffle(all_users_ids)

        return all_users_ids

    def _generate_order_lines(
        self,
        products: dict,
        user_ids: list[int],
        num_orders: int,
        max_num_items: int,
        date_created: datetime,
    ) -> list[OrdersModel]:
        """
        Creates order lines by assigning random products and quantities to a series of user orders.

        For each order a random user ID is assigned from the provided list and a number of random products are assigned based on product popularity weights.

        Args:
            products (dict): A dictionary of item_sku keys and item_price and item_popularity values.
            user_ids (list[int]): A list of user IDs.
            num_orders (int): The total number of orders to generate.
            max_num_items (int): Maximum number of items allowed per order.
            date_created (datetime): The date the order was created.

        Returns:
            list[OrdersModel]: A list of OrdersModel instances representing all order lines across the generated orders.

        """
        last_order_id = self._get_last_order_id()
        skus = list(products)
        popularities = [data["item_popularity"] for product, data in products.items()]

        order_models: list[OrdersModel] = []

        for i in range(num_orders):
            items_in_order = self._get_random_num_items(max_num_items)
            order_id = (last_order_id + 1) + i
            order_lines = {}

            for _ in range(items_in_order):
                random_product = random.choices(skus, popularities)[0]

                if random_product in order_lines:
                    order_lines[random_product] += 1
                else:
                    order_lines[random_product] = 1

            for item_sku, qty in order_lines.items():
                price = products[item_sku]["item_price"]
                order_line = OrdersModel(
                    order_id=order_id,
                    user_id=user_ids[i],
                    item_sku=item_sku,
                    qty=qty,
                    item_price=price,
                    date_created=date_created,
                )
                order_models.append(order_line)

        return order_models

    def get_count_orders(self) -> int:
        """
        Get the total number of orders in the database.

        Returns:
            int: Total order count.

        """
        with get_session() as db:
            count_orders = db.query(OrdersModel).count()
        return count_orders

    def get_orders(
        self,
        order_id: int | list[int] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[Order]:
        """
        Retrieve orders from the database with optional filters.

        Args:
            order_id (int | list[int] | None): An order ID or list of Order ID's.
            start_date (str | None): Start date (inclusive) in 'YYYY-MM-DD' format.
            end_date (str | None): End date (inclusive) in 'YYYY-MM-DD' format.

        Returns:
            list[Order]: List of Order dataclass instances matching the filters.

        """
        log.debug(
            "Fetching orders with filters - order_id: %s, start_date: %s, end_date: %s.",
            order_id,
            start_date,
            end_date,
        )
        if start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

        if end_date:
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        with get_session() as db:
            query = db.query(OrdersModel)

            if start_date:
                query = query.filter(func.date(OrdersModel.date_created) >= start_date)

            if end_date:
                query = query.filter(func.date(OrdersModel.date_created) <= end_date)

            if order_id is not None:
                if isinstance(order_id, list):
                    query = query.filter(OrdersModel.order_id.in_(order_id))
                else:
                    query = query.filter(OrdersModel.order_id == order_id)

            return [order.to_plain() for order in query.all()]

    def _introduce_messy_data(self, orders: list[Order]) -> list[tuple]:
        """
        Introduces a small randomised amount of dirty data to the order data.

        Args:
            orders (list[Order]): A list of Order dataclass instances.

        Returns:
            list[tuple]: A list of tuples representing rows of order lines with messy data.

        """
        messy_orders = []

        for order in orders:
            messy_order = list(astuple(order))

            # Change date strings
            if random.random() < 0.05:
                if random.random() < 0.5:
                    messy_order[6] = messy_order[6].strftime("%d/%m/%Y")
                else:
                    messy_order[6] = messy_order[6].strftime("%d-%m-%Y")

            # Introduce blank values
            if random.random() < 0.1:
                idx = random.randint(5, 6)
                messy_order[idx] = None

            messy_order = tuple(messy_order)

            # Duplicate order rows
            if random.random() < 0.02:
                messy_orders.append(messy_order)

            messy_orders.append(messy_order)

        return messy_orders

    def to_csv(
        self,
        order_id: int | list[int] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        timestamp: str = datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        *,
        messy_data: bool = False,
    ) -> None:
        """
        Export order data to a CSV file locally and/or to Google Cloud Storage depending on the env config.

        Args:
            order_id (int | list[int] | None): An order ID or list of order ID's.
            start_date (str | None): Start date (inclusive) in 'YYYY-MM-DD' format.
            end_date (str | None): End date (inclusive) in 'YYYY-MM-DD' format.
            timestamp (str): The timestamp for the csv filename.
            messy_data (bool): If True, introduces a randomised amount of 'dirty' data to the order data.

        """
        export_data = self.get_orders(order_id, start_date, end_date)
        log.debug(
            "Exporting %s orders to CSV, messy_data=%s",
            len(export_data),
            messy_data,
        )
        file_path = f"Order_report_{timestamp}.csv"

        if len(export_data) == 0:
            return
        if messy_data:
            export_data = self._introduce_messy_data(export_data)
        if config.CSV_LOCAL_FILE:
            self._save_to_file(export_data, file_path)
        if config.CSV_CLOUD_STORAGE_FILE:
            self._save_to_cloud_storage(export_data, file_path)

    def _save_to_file(self, export_data: list[Order] | list[tuple], file_path: str) -> None:
        """
        Save order data to a local CSV file.

        Args:
            export_data (list[Order] | list[tuple]): List of Order dataclass instances or list of tuples of messy order data.
            file_path (str): Path to the output CSV file.

        """
        if len(export_data) == 0:
            log.debug("No order export_data. Skipping CSV generation.")
            return
        log.debug("Saving %s orders to local CSV file at %s.", len(export_data), file_path)

        with open(file_path, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "order_line_id",
                    "order_id",
                    "user_id",
                    "item_sku",
                    "qty",
                    "item_price",
                    "date_created",
                ],
            )

            for row in export_data:
                if isinstance(row, Order):
                    writer.writerow(astuple(row))
                else:
                    writer.writerow(row)
        log.debug("Saved order data to local file: %s.", file_path)

    def _save_to_cloud_storage(
        self,
        export_data: list[Order] | list[tuple],
        file_path: str,
    ) -> None:
        """
        Upload order data as a CSV to a Google Cloud Storage bucket.

        Args:
            export_data (list[Order] | list[tuple]): List of Order dataclass instances or list of tuples of messy order data.
            file_path (str): File name to use in the cloud storage bucket.

        """
        if len(export_data) == 0:
            log.debug("No order export_data. Skipping CSV upload.")
            return
        log.debug("Uploading %s orders to cloud storage CSV file at %s.", len(export_data), file_path)

        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(
            [
                "order_line_id",
                "order_id",
                "user_id",
                "item_sku",
                "qty",
                "item_price",
                "date_created",
            ],
        )

        for row in export_data:
            if isinstance(row, Order):
                writer.writerow(astuple(row))
            else:
                writer.writerow(row)

        upload_data = csv_buffer.getvalue()

        upload_to_bucket(
            f"order_reports/{file_path}",
            upload_data,
            config.STORAGE_BUCKET,
        )
        log.debug("Uploaded order CSV to cloud storage: %s.", file_path)

    def _get_last_order_id(self) -> int:
        """
        Retrieves the index of the last order ID.

        Returns:
            int: The number of the most recent order ID.

        """
        with get_session() as db:
            last_order_id = db.query(OrdersModel.order_id).order_by(OrdersModel.order_id.desc()).limit(1).scalar()
        log.debug("Last order ID fetched: %s", last_order_id)
        return last_order_id if last_order_id is not None else 0

    def _get_random_num_items(self, max_num_items: int) -> int:
        """
        Returns a random integer between 0 and max_num_items with a bias towards smaller numbers.

        Args:
            max_num_items (int): The maximum amount of items in an order.

        Returns:
            int: The number of items to generate in an order.

        """
        max_num_items_list = list(range(1, max_num_items + 1))
        scaling = 0.6
        inv_exp_list = [1 / math.exp(x * scaling) for x in max_num_items_list]
        total = sum(inv_exp_list)
        weighting = [value / total for value in inv_exp_list]
        num_items = random.choices(max_num_items_list, weights=weighting)[0]
        return num_items
