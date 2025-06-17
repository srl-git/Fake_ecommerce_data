import csv
import io
import random
from dataclasses import astuple
from datetime import datetime, timedelta, timezone

from sqlalchemy import func
from sqlalchemy.sql import expression

from data_generator.google_cloud_storage import upload_to_bucket
from shared.config import get_config
from shared.db_connection import get_session
from shared.db_models import Product, ProductsModel
from shared.logger import get_logger

log = get_logger(__name__)
config = get_config()


class Products:
    """A class to generate, retrieve, and export product data using SQLAlchemy."""

    def create(
        self,
        label_prefix: str | list[str],
        preorder_weeks: int,
        num_items: int,
        pricing: list[float],
        creation_date: str | datetime,
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
        if num_items == 0:
            log.debug("Skipping generating products, num_items= %s", num_items)
            return None

        log.debug(
            "Generating products with parameters - num_items: %s, label_prefix: %s, preorder_weeks: %s, pricing: %s, creation_date: %s .",
            num_items,
            label_prefix,
            preorder_weeks,
            pricing,
            creation_date,
        )

        if not isinstance(creation_date, datetime):
            creation_date = datetime.strptime(creation_date, "%Y-%m-%d").astimezone(timezone.utc)

        if isinstance(label_prefix, list):
            label_prefix = random.choice(label_prefix)

        release_date = creation_date + timedelta(weeks=preorder_weeks)
        popularity_upper_limit = self._get_upper_limit()

        index = self._get_sku_index(label_prefix)

        product_models: list[ProductsModel] = []

        for i in range(num_items):
            product_model = ProductsModel(
                item_sku=f"{label_prefix}{index + 1 + i:03}",
                item_price=random.choice(pricing),
                release_date=release_date,
                date_created=creation_date,
                date_updated=creation_date,
                active=True,
                item_popularity=random.uniform(0.0, popularity_upper_limit),
            )
            product_models.append(product_model)

        with get_session() as db:
            db.add_all(product_models)
            db.flush()
            log.debug("Flushed %s products to the database.", len(product_models))
            products = [product.to_plain() for product in product_models]

        self._set_popularity_scores()

        log.info("%s products added to the database.", len(products))

        return products

    def get_count_products(self) -> int:
        """
        Get the total number of products in the database.

        Returns:
            int: Total product count.

        """
        with get_session() as db:
            count_products = db.query(ProductsModel).count()
        return count_products

    def get_products(
        self,
        item_sku: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[Product]:
        """
        Retrieve products from the database with optional filters.

        Args:
            item_sku (str | list[str] | None): A product catalogue number (item sku).
            start_date (str | None): Start date (inclusive) in 'YYYY-MM-DD' format.
            end_date (str | None): End date (inclusive) in 'YYYY-MM-DD' format.

        Returns:
            list[Product]: List of Product dataclass instances matching the filters.

        """
        log.debug(
            "Fetching products with filters - item_sku: %s, start_date: %s, end_date: %s.",
            item_sku,
            start_date,
            end_date,
        )
        if start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

        if end_date:
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        with get_session() as db:
            query = db.query(ProductsModel)

            if start_date:
                query = query.filter(func.date(ProductsModel.date_created) >= start_date)

            if end_date:
                query = query.filter(func.date(ProductsModel.date_created) <= end_date)

            if item_sku is not None:
                if isinstance(item_sku, list):
                    query = query.filter(ProductsModel.item_sku.in_(item_sku))
                else:
                    query = query.filter(ProductsModel.item_sku == item_sku)

            return [product.to_plain() for product in query.all()]

    def to_csv(
        self,
        item_sku: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        timestamp: str = datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    ) -> None:
        """
        Export product data to a CSV file locally and/or to Google Cloud Storage depending on the env config.

        Args:
            item_sku (str | list[str] | None, optional): The product catalogue number (item sku).
            start_date (str | None): Start date (inclusive) in 'YYYY-MM-DD' format.
            end_date (str | None): End date (inclusive) in 'YYYY-MM-DD' format.
            timestamp (str): The timestamp for the csv filename.

        """
        export_data = [astuple(product)[:-1] for product in self.get_products(item_sku, start_date, end_date)]
        log.debug("Exporting %s products to CSV.", len(export_data))
        file_path = f"Product_report_{timestamp}.csv"

        if len(export_data) == 0:
            return
        if config.CSV_LOCAL_FILE:
            self._save_to_file(export_data, file_path)
        if config.CSV_CLOUD_STORAGE_FILE:
            self._save_to_cloud_storage(export_data, file_path)

    def _save_to_file(self, export_data: list[tuple], file_path: str) -> None:
        """
        Save product data to a local CSV file.

        Args:
            export_data (list[tuple]): List of tuples of product data.
            file_path (str): Path to the output CSV file.

        """
        if len(export_data) == 0:
            log.debug("No product export_data. Skipping CSV generation.")
            return
        log.debug("Saving %s products to local CSV file at %s.", len(export_data), file_path)

        with open(file_path, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "Product SKU",
                    "Price",
                    "Release Date",
                    "Date Created",
                    "Date Updated",
                    "Active",
                ],
            )

            for row in export_data:
                writer.writerow(row)
        log.debug("Saved product data to local file: %s.", file_path)

    def _save_to_cloud_storage(self, export_data: list[tuple], file_path: str) -> None:
        """
        Upload product data as a CSV to a Google Cloud Storage bucket.

        Args:
            export_data (list[tuple]): List of tuples of product data.
            file_path (str): File name to use in the cloud storage bucket.

        """
        if len(export_data) == 0:
            log.debug("No product export_data. Skipping CSV upload.")
            return
        log.debug("Uploading %s products to cloud storage CSV file at %s.", len(export_data), file_path)

        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(
            [
                "Product SKU",
                "Price",
                "Release Date",
                "Date Created",
                "Date Updated",
                "Active",
            ],
        )

        for row in export_data:
            writer.writerow(row)

        upload_data = csv_buffer.getvalue()

        upload_to_bucket(
            f"product_reports/{file_path}",
            upload_data,
            config.STORAGE_BUCKET,
        )
        log.debug("Uploaded product CSV to cloud storage: %s.", file_path)

    def _get_sku_index(self, label_prefix: str) -> int:
        """
        Retrieves the next available index for a new SKU based on a given prefix.

        Args:
            label_prefix (str): The prefix of the SKU to search for.

        Returns:
            The count of existing SKUs matching the prefix, which serves as the
            next available index for a new SKU with that prefix.

        """
        with get_session() as db:
            sku_index = (
                db.query(func.count(ProductsModel.item_sku))
                .filter(ProductsModel.item_sku.like(f"{label_prefix}%"))
                .scalar()
            )
        log.debug("Found %s existing SKUs with prefix '%s'.", sku_index, label_prefix)

        return sku_index

    def _get_upper_limit(self) -> float:
        """
        Calculates an upper limit for new product popularity scores.

        This method queries the database for the highest product popularity score.
        If a maximum score exists, it's multiplied by 1.5 to set a new upper bound. This ensures
        that newly created products will have higher popularity scores compared
        to existing products.

        Returns:
            float: The calculated upper limit for product popularity scores.

        """
        with get_session() as db:
            max_popularity_score = db.query(
                func.max(ProductsModel.item_popularity),
            ).scalar()
        popularity_upper_limit = max_popularity_score * 1.5 if max_popularity_score else 1
        log.debug(
            "Max popularity score in database: %s, upper limit set to: %s",
            max_popularity_score,
            popularity_upper_limit,
        )

        return popularity_upper_limit

    def _set_popularity_scores(self) -> None:
        """
        Normalises all product popularity scores in the database.

        This scales all `item_popularity` values so they sum to 1,
        making them suitable for use as probability weights for random selection.
        Handles cases where total popularity is zero or no products exist.

        """
        with get_session() as db:
            total = db.query(func.sum(ProductsModel.item_popularity)).scalar()
            if total is None or total == 0:
                log.debug("Total popularity score is zero or None. Skipping normalization.")
                return
            log.debug("Total popularity score before normalisation: %s", total)
            normaliser = 1 / float(total)
            log.debug("Applying normalisation factor: %s", normaliser)

            db.execute(
                expression.update(ProductsModel).values(
                    item_popularity=ProductsModel.item_popularity * normaliser,
                ),
            )
