from datetime import datetime
from unidecode import unidecode
import random
import csv
import io

from faker import Faker
from faker.config import AVAILABLE_LOCALES

from database.DatabaseConnection import DatabaseConnection
from google_cloud import upload_to_bucket
import sql_statements as sql

class Users:

    def __init__(self, db_path) -> None:
        
        if not (isinstance(db_path, str) and db_path.endswith('.db')):
            raise ValueError('The path to the database file must be a non empty string with a .db file extension')
        
        self.db_path = db_path
        self._initialise_db_table()
    
    def __repr__(self) -> str:
        
        return f'Users({self.db_path})'

    def __str__(self) -> str:
    
        return f'There are {self.get_count_users()} users in the database'
    
    def create(
        self,
        num_users: int,
        locales: list[str]
    ) -> list[tuple]: 

        if num_users == 0:
            return []
        
        self._validate_create_args(num_users, locales)

        date_created = datetime.now().strftime('%Y-%m-%d')

        locale_weighting = [random.uniform(0.0, 1) for _ in range(len(locales))]
        normalised_locale_weighting = [w / sum(locale_weighting) for w in locale_weighting]
        faker_instances = {locale: Faker(locale) for locale in locales}
        users = []

        last_user_id = self._get_last_user_id()

        for i in range(num_users):

            user_id = (last_user_id + 1) + i
            random_locale = random.choices(locales, normalised_locale_weighting)[0]
            fake = faker_instances[random_locale]
            profile = fake.simple_profile()
            user_name = str(profile['name'])
            user_address = str(profile['address']).replace('\n',', ')
            user_country = fake.current_country()
            user_email = self._create_email(user_name)
            user = (user_id, user_name, user_address, user_country, user_email, date_created)
            users.append(user)

        self._add_to_db(users)
        
        return users

    def get_count_users(self) -> int:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.user_statements.get_count_users)
            count_users = db.cursor.fetchone()[0]
            
        return count_users
        
    def get_users(
        self,
        user_id: int | list[int] | tuple[int] | None = None
    ) -> list[tuple]:
        
        with DatabaseConnection(self.db_path) as db:
            if user_id:
                if isinstance(user_id, int):
                    user_id = (user_id, )
                db.cursor.execute(sql.user_statements.get_users_by_id(user_id), user_id)
                users = db.cursor.fetchall()
            
            else:    
                db.cursor.execute(sql.user_statements.get_users)
                users = db.cursor.fetchall()
                
        return users

    def get_users_by_date_range(
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
                'ERROR in Users.get_users_by_date_range()). '
                'Expected a datetime object or a valid date string in format YYYY-MM-DD for start_date and end_date arguments.'
            )
        
        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.user_statements.get_users_by_date_range,(start_date, end_date))
            users_by_date_range = db.cursor.fetchall()
            
        return users_by_date_range
    
    def get_random_users(self, num_previous_users: int) -> list[tuple]:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute('''
            SELECT *
            FROM Users
            ORDER BY RANDOM()
            LIMIT ?
        ''',(num_previous_users,))

            random_users = db.cursor.fetchall()

        return random_users
        
    def to_csv(
        self,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        local_file: bool = True,
        cloud_storage_file: bool = False
    ) -> None:

        if start_date or end_date:
            try:
                export_data = self.get_users_by_date_range(start_date, end_date)
                
                if isinstance(start_date, datetime):
                    start_date = start_date.strftime('%Y-%m-%d')
                if isinstance(end_date, datetime):
                    end_date = end_date.strftime('%Y-%m-%d')
                
                start_date = start_date if start_date else ''
                end_date = f'_{end_date}' if end_date else ''
                file_path = f'User_report_{start_date}{end_date}.csv'
                
            except ValueError:
                raise ValueError(
                    'ERROR in Users.to_csv(). '
                    'Expected a string in date format YYYY-MM-DD for start_date and end_date arguments.'
                )
        else:
            export_data = self.get_users()
            date_today = datetime.today().date().strftime('%Y-%m-%d')
            file_path = f'User_report_{date_today}.csv'

        if local_file:
            self._save_to_file(export_data, file_path)
        if cloud_storage_file:
            self._save_to_cloud_storage(export_data, file_path)
    
    def _save_to_file(self, export_data: list[tuple], file_path: str):

        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['user_id', 'user_name', 'user_address', 'user_country', 'user_email', 'date_created', 'date_updated'])
            
            for row in export_data:
                writer.writerow(row)
    
    def _save_to_cloud_storage(self, export_data: list[tuple], file_path: str):

        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(['user_id', 'user_name', 'user_address', 'user_country', 'user_email', 'date_created', 'date_updated'])

        for row in export_data:
            writer.writerow(row)
        
        upload_data = csv_buffer.getvalue()

        upload_to_bucket(f'user_reports/{file_path}', upload_data, 'srl_ecommerce')

    def _create_email(self, name: str) -> str:
        email_prefix = unidecode(name.lower().replace(' ','').replace('.',''))
        email = f'{email_prefix}@example.com'

        return email
    
    def _initialise_db_table(self) -> None:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.user_statements.create_user_table)

    def _drop_db_table(self) -> None:
        
        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.user_statements.drop_user_table)

    def _add_to_db(self, users: list[tuple]) -> None:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.executemany(sql.user_statements.add_users_to_db, users)

    def _get_last_user_id(self) -> int:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.user_statements.get_last_user_id)
            result = db.cursor.fetchone()
            last_user_id = result[0] if result is not None else 0
            
        return last_user_id
    
    def _validate_create_args(
            self, 
            num_users: int, 
            locales: list[str]
    ) -> None:

        if not isinstance(num_users, int) or isinstance(num_users, bool):
            raise TypeError(
                'ERROR in Users.create(). '
                'Expected an integer for num_users argument. '
                f'Received value: "{num_users}" of type: {type(num_users).__name__}.'
            )
        if not (isinstance(locales, list) and all(isinstance(s, str) for s in locales)):
            raise ValueError(
                'ERROR in Users.create(). Invalid locale format. '
                'Expected a string or list of strings in the format ["en_GB", "fr_FR", "de_DE"].'
            )
        if not all (locale in AVAILABLE_LOCALES for locale in locales):
            raise ValueError(
                'ERROR in Users.create(). Invalid locale. '
                f'Expected one or many values from the available locales: \n {sorted(AVAILABLE_LOCALES)}.'
            )

