import csv
import io
import random
from dataclasses import astuple
from datetime import datetime, timezone

from faker import Faker
from faker.config import AVAILABLE_LOCALES
from sqlalchemy import func
from unidecode import unidecode

from data_generator.google_cloud_storage import upload_to_bucket
from shared.config import get_config
from shared.db_connection import get_session
from shared.db_models import User, UsersModel
from shared.logger import get_logger

log = get_logger(__name__)
config = get_config()


class Users:
    """
    A class to generate, retrieve, and export user data using the Faker library and SQLAlchemy.

    Attributes:
        locales (list[str]): List of locale codes to be used for generating fake user data.

    """

    def __init__(self, locales: list[str]) -> None:
        """
        Initialise the User class with a list of locales for fake data generation.

        Args:
            locales (list[str]): List of Faker locales (en_US, fr_FR...)

        Raises:
            ValueError: If a locale doesn't match an available locale in Faker.

        """
        if not all(locale in AVAILABLE_LOCALES for locale in locales):
            error_msg = f"Expected one or many values from the available locales: \n {sorted(AVAILABLE_LOCALES)}."
            raise ValueError(error_msg)
        self.locales = locales
        log.debug("Users initialized with locales: %s", self.locales)

    def create(self, num_users: int, date_created: datetime) -> list[User] | None:
        """
        Generates and adds fake users to the database.

        Args:
            num_users (int): Number of users to generate.
            date_created (datetime): The date the user was created.

        Returns:
            list[User]: A list of created User dataclass instances.

        """
        if num_users == 0:
            log.debug("Skipping generating users, num_users= %s", num_users)
            return None

        log.debug("Generating %s users.", num_users)

        locale_weighting = [random.uniform(0.0, 1) for _ in range(len(self.locales))]
        normalised_locale_weighting = [w / sum(locale_weighting) for w in locale_weighting]
        faker_instances = {locale: Faker(locale) for locale in self.locales}
        user_models: list[UsersModel] = []

        for _ in range(num_users):
            random_locale = random.choices(self.locales, normalised_locale_weighting)[0]
            fake = faker_instances[random_locale]
            profile = fake.simple_profile()
            user_name = str(profile["name"])

            user_model = UsersModel(
                user_name=user_name,
                user_address=str(profile["address"]).replace("\n", ", "),
                user_country=fake.current_country(),
                user_email=self._create_email(user_name),
                date_created=date_created,
            )
            user_models.append(user_model)

        with get_session() as db:
            db.add_all(user_models)
            db.flush()
            log.debug("Flushed %s users to the database.", len(user_models))
            users = [user.to_plain() for user in user_models]

        log.info("%s new users created.", len(users))

        return users

    def get_count_users(self) -> int:
        """
        Get the total number of users in the database.

        Returns:
            int: Total user count.

        """
        with get_session() as db:
            count_users = db.query(UsersModel).count()
        return count_users

    def get_users(
        self,
        user_id: int | list[int] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[User]:
        """
        Retrieve users from the database with optional filters.

        Args:
            user_id (int | list[int] | None): User ID or list of user IDs to filter.
            start_date (str | None): Start date (inclusive) in 'YYYY-MM-DD' format.
            end_date (str | None): End date (inclusive) in 'YYYY-MM-DD' format.

        Returns:
            list[User]: List of User dataclass instances matching the filters.

        """
        log.debug(
            "Fetching users with filters - user_id: %s, start_date: %s, end_date: %s.",
            user_id,
            start_date,
            end_date,
        )
        if start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

        if end_date:
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        with get_session() as db:
            query = db.query(UsersModel)

            if start_date:
                query = query.filter(func.date(UsersModel.date_created) >= start_date)

            if end_date:
                query = query.filter(func.date(UsersModel.date_created) <= end_date)

            if user_id is not None:
                if isinstance(user_id, list):
                    query = query.filter(UsersModel.user_id.in_(user_id))
                else:
                    query = query.filter(UsersModel.user_id == user_id)

            return [user.to_plain() for user in query.all()]

    def to_csv(
        self,
        user_id: int | list[int] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        timestamp: str = datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    ) -> None:
        """
        Export user data to a CSV file locally and/or to Google Cloud Storage depending on the env config.

        Args:
            user_id (int | list[int] | None): User ID or list of user IDs to filter.
            start_date (str | None): Start date (inclusive) in 'YYYY-MM-DD' format.
            end_date (str | None): End date (inclusive) in 'YYYY-MM-DD' format.
            timestamp (str): The timestamp for the csv filename.

        """
        export_data = self.get_users(user_id, start_date, end_date)
        log.debug("Exporting %s users to CSV.", len(export_data))
        file_path = f"User_report_{timestamp}.csv"

        if len(export_data) == 0:
            return
        if config.CSV_LOCAL_FILE:
            self._save_to_file(export_data, file_path)
        if config.CSV_CLOUD_STORAGE_FILE:
            self._save_to_cloud_storage(export_data, file_path)

    def _save_to_file(self, export_data: list[User], file_path: str) -> None:
        """
        Save user data to a local CSV file.

        Args:
            export_data (list[User]): List of User dataclass instances.
            file_path (str): Path to the output CSV file.

        """
        if len(export_data) == 0:
            log.debug("No user export_data. Skipping CSV generation.")
            return
        log.debug("Saving %s users to local CSV file at %s.", len(export_data), file_path)

        with open(file_path, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "user_id",
                    "user_name",
                    "user_address",
                    "user_country",
                    "user_email",
                    "date_created",
                ],
            )

            for row in export_data:
                writer.writerow(astuple(row))
        log.debug("Saved user data to local file: %s.", file_path)

    def _save_to_cloud_storage(self, export_data: list[User], file_path: str) -> None:
        """
        Upload user data as a CSV to a Google Cloud Storage bucket.

        Args:
            export_data (list[User]): List of User dataclass instances.
            file_path (str): File name to use in the cloud storage bucket.

        """
        if len(export_data) == 0:
            log.debug("No user export_data. Skipping CSV upload.")
            return
        log.debug("Uploading %s users to cloud storage CSV file at %s.", len(export_data), file_path)

        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(
            [
                "user_id",
                "user_name",
                "user_address",
                "user_country",
                "user_email",
                "date_created",
            ],
        )

        for row in export_data:
            writer.writerow(astuple(row))

        upload_data = csv_buffer.getvalue()

        upload_to_bucket(
            f"user_reports/{file_path}",
            upload_data,
            config.STORAGE_BUCKET,
        )
        log.debug("Uploaded user CSV to cloud storage: %s.", file_path)

    def _create_email(self, name: str) -> str:
        """
        Generate an email address from a user's name.

        Args:
            name (str): User's full name.

        Returns:
            str: Generated email address in the format '<name>@example.com'.

        """
        email_prefix = unidecode(name).lower().replace(" ", "").replace(".", "")
        email = f"{email_prefix}@example.com"
        return email
