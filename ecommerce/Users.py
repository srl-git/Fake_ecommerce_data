from datetime import datetime
from unidecode import unidecode
import random
import csv

from faker import Faker
from faker.config import AVAILABLE_LOCALES

from database.DatabaseConnection import DatabaseConnection
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
        locales: list[str],
        creation_date: str | datetime = datetime.now()
    ) -> None: 

        self._validate_create_args(num_users, locales, creation_date)
        
        if isinstance(creation_date, datetime):
            creation_date = creation_date.strftime('%Y-%m-%d')

        popularity_upper_limit = self._get_upper_limit()
        locale_weighting = [random.uniform(0.0, 1) for _ in range(len(locales))]
        normalised_locale_weighting = [w / sum(locale_weighting) for w in locale_weighting]
        faker_instances = {locale: Faker(locale) for locale in locales}
        users = []

        for i in range(num_users):

            random_locale = random.choices(locales, normalised_locale_weighting)[0]
            fake = faker_instances[random_locale]
            profile = fake.simple_profile()
            user_name = str(profile['name'])
            user_address = str(profile['address']).replace('\n',', ')
            user_country = fake.current_country()
            user_email = self._create_email(user_name)
            date_created = date_updated = creation_date
            user_weighting = random.uniform(0.0, popularity_upper_limit)
            user = (user_name, user_address, user_country, user_email, date_created, date_updated, user_weighting)
            users.append(user)

        self._add_to_db(users)

    def update(
        self,
        user_id: int | list[int] | tuple[int],
        user_name: str | list[str] | None = None,
        user_address: str | list[str] | None = None,
        user_country: str | list[str] | None = None,
        user_email: str | list[str] | None = None
    ) -> None:
        
        self._validate_update_args(user_id, user_name, user_address, user_country, user_email)

        if isinstance(user_id, int):
            user_id = (user_id, )

        users_to_update = self.get_users(user_id)

        if isinstance(user_name, str):
            user_name = [user_name] * len(users_to_update)

        if isinstance(user_address, str):
            user_address = [user_address] * len(users_to_update)

        if isinstance(user_country, str):
            user_country = [user_country] * len(users_to_update)

        if isinstance(user_email, str):
            user_email = [user_email] * len(users_to_update)

        update_data = []

        if users_to_update:
            for user in users_to_update:

                user_id_to_update = user[0]
                index = user_id.index(user_id_to_update)
                user_name_updated = user_name[index] if user_name is not None else user[1]
                user_address_updated = user_address[index] if user_address is not None else user[2]
                user_country_updated = user_country[index] if user_country is not None else user[3]
                user_email_updated = user_email[index] if user_email is not None else user[4]
                date_updated = datetime.today().strftime('%Y-%m-%d')
                update_data.append((user_name_updated, user_address_updated, user_country_updated, user_email_updated, date_updated, user_id_to_update))

            with DatabaseConnection(self.db_path) as db:
                db.cursor.executemany(sql.user_statements.update_users,(update_data))

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

    def get_last_added(self) -> tuple:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.user_statements.get_last_added)
            last_added = db.cursor.fetchall()[0]

        return last_added

    def get_last_updated(self) -> tuple:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.user_statements.get_last_updated)
            last_updated = db.cursor.fetchall()[0]

        return last_updated
    
    def to_csv(
        self,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None
    ) -> None:

        if start_date or end_date:
            try:
                export_data = [user[:-1] for user in self.get_users_by_date_range(start_date, end_date)]
                
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
            export_data = [user[:-1] for user in self.get_users()]
            date_today = datetime.today().date().strftime('%Y-%m-%d')
            file_path = f'User_report_{date_today}.csv'

        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['user_id', 'user_name', 'user_address', 'user_country', 'user_email', 'date_created', 'date_updated'])
            
            for row in export_data:
                writer.writerow(row)        
   
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

        self._set_popularity_scores()

    def _get_upper_limit(self) -> float:

        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.user_statements.get_upper_limit)
            max_popularity_score = db.cursor.fetchone()[0]

        popularity_upper_limit = max_popularity_score * 1.5 if max_popularity_score else 1

        return popularity_upper_limit
    
    def _set_popularity_scores(self) -> None:
 
        with DatabaseConnection(self.db_path) as db:
            db.cursor.execute(sql.user_statements.get_popularity_scores)
            old_popularity_scores = tuple(score[0] for score in db.cursor.fetchall())

        normalizer = 1 / float(sum(old_popularity_scores))
        new_popularity_scores = tuple(score * normalizer for score in old_popularity_scores)
        update_scores = zip(new_popularity_scores, old_popularity_scores)

        with DatabaseConnection(self.db_path) as db:
            db.cursor.executemany(sql.user_statements.set_popularity_scores, (update_scores))

    def _validate_create_args(
            self, 
            num_users: int, 
            locales: list[str],
            creation_date: str | datetime
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

        try:
            if not isinstance(creation_date, datetime):
                datetime.fromisoformat(creation_date)
        except ValueError:
            raise ValueError(
                'ERROR in Users.create(). '
                'Expected a datetime object or valid date string in format YYYY-MM-DD for creation_date argument.'
            )

    def _validate_update_args( 
        self, 
        user_id: int | list[int] | tuple[int],
        user_name: str | list[str] | None,
        user_address: str | list[str] | None,
        user_country: str | list[str] | None,
        user_email: str | list[str] | None) -> None:

        if not (user_id and isinstance(user_id, (list, int))):
            raise ValueError(
                'ERROR in Users.update(). Expected an integer or list/tuple of integers for user_id argument. '
                f'Received value "{user_id}" of type: {type(user_id).__name__}.'
            )
        
        invalid_user_ids = None

        if isinstance(user_id, (list, tuple)):
            if not all(isinstance(element, int) and element > 0 for element in user_id):
                raise ValueError(
                    'ERROR in Users.update(). '
                    'Invalid value in user_id argument: All elements in the list/tuple must be positive integers.'
                )
            invalid_user_ids = [id for id in user_id if id not in set(user[0] for user in self.get_users(user_id))]

        if isinstance(user_id, int):
            invalid_user_ids = user_id if user_id not in set(user[0] for user in self.get_users(user_id)) else None
            
        if invalid_user_ids:
            raise ValueError(
                'ERROR in Users.update(). The following user_id(s) do not exist in the database:\n'
                f'{invalid_user_ids}'
            )
        
        if user_name is not None:
        
            if not isinstance(user_name, (list, str)):
                raise TypeError(
                    'ERROR in Users.update(). Expected a string or list of strings for user_name argument. '
                    f'Received value "{user_name}" of type: {type(user_name).__name__}.'
                )
                
            if isinstance(user_name, list):

                if isinstance(user_id, int):
                    raise ValueError(
                        'ERROR in Users.update(). Mismatch between user_name and user_id: '
                        f'Expected 1 user_name but got {len(user_name)}.'
                    )
                
                if len(user_name) not in (1, len(user_id)):
                    raise ValueError(
                        'ERROR in Users.update(). Mismatch between user_name and user_id: '
                        f'Expected either 1 or {len(user_id)} user_name values, but got {len(user_name)}.'
                    )
                
                if not all(isinstance(element, str) and len(element) >= 3 for element in user_name):
                    raise ValueError(
                        'ERROR in Users.update(). '
                        'Invalid value in user_name argument: All elements in the list must be strings of at least 3 letters.'
                    )
                
            if isinstance(user_name, str):
                if len(user_name) < 3:
                    raise ValueError(
                        'ERROR in Users.update(). '
                        'user_name must be at least 3 letters long.'
                    )
                
        if user_address is not None:
    
            if not isinstance(user_address, (list, str)):
                raise TypeError(
                    'ERROR in Users.update(). Expected a string or list of strings for user_address argument. '
                    f'Received value "{user_address}" of type: {type(user_address).__name__}.'
                )
                
            if isinstance(user_address, list):

                if isinstance(user_id, int):
                    raise ValueError(
                        'ERROR in Users.update(). Mismatch between user_address and user_id: '
                        f'Expected 1 user_name but got {len(user_address)}.'
                    )
                
                if len(user_address) not in (1, len(user_id)):
                    raise ValueError(
                        'ERROR in Users.update(). Mismatch between user_address and user_id: '
                        f'Expected either 1 or {len(user_id)} user_address values, but got {len(user_address)}.'
                    )
                
                if not all(isinstance(element, str) and len(element) >= 6 for element in user_address):
                    raise ValueError(
                        'ERROR in Users.update(). '
                        'Invalid value in user_address argument: All elements in the list must be strings of at least 6 letters.'
                    )
                    
                if isinstance(user_address, str):
                    if len(user_address) < 6:
                        raise ValueError(
                            'ERROR in Users.update(). '
                            'user_address must be at least 6 letters long.'
                        )  
            
        if user_country is not None:
    
            if not isinstance(user_country, (list, str)):
                raise TypeError(
                    'ERROR in Users.update(). Expected a string or list of strings for user_country argument. '
                    f'Received value "{user_country}" of type: {type(user_country).__name__}.'
                )
                
            if isinstance(user_country, list):

                if isinstance(user_id, int):
                    raise ValueError(
                        'ERROR in Users.update(). Mismatch between user_country and user_id: '
                        f'Expected 1 user_country but got {len(user_country)}.'
                    )
                
                if len(user_country) not in (1, len(user_id)):
                    raise ValueError(
                        'ERROR in Users.update(). Mismatch between user_country and user_id: '
                        f'Expected either 1 or {len(user_id)} user_country values, but got {len(user_country)}.'
                    )
                
                if not all(isinstance(element, str) and len(element) >= 2 for element in user_country):
                    raise ValueError(
                        'ERROR in Users.update(). '
                        'Invalid value in user_country argument: All elements in the list must be strings of at least 2 letters.'
                    )
                
            if isinstance(user_country, str):
                if len(user_country) < 2:
                    raise ValueError(
                        'ERROR in Users.update(). '
                        'user_country must be at least 2 letters long.'
                    )

        if user_email is not None:
    
            if not isinstance(user_email, (list, str)):
                raise TypeError(
                    'ERROR in Users.update(). Expected a string or list of strings for user_email argument. '
                    f'Received value "{user_email}" of type: {type(user_email).__name__}.'
                )
                
            if isinstance(user_email, list):

                if isinstance(user_id, int):
                    raise ValueError(
                        'ERROR in Users.update(). Mismatch between user_email and user_id: '
                        f'Expected 1 user_country but got {len(user_email)}.'
                    )
                
                if len(user_email) not in (1, len(user_id)):
                    raise ValueError(
                        'ERROR in Users.update(). Mismatch between user_email and user_id: '
                        f'Expected either 1 or {len(user_id)} user_country values, but got {len(user_email)}.'
                    )
                
                if not all(isinstance(element, str) and len(element) >= 2 for element in user_email):
                    raise ValueError(
                        'ERROR in Users.update(). '
                        'Invalid value in user_email argument: All elements in the list must be strings of at least 4 letters.'
                    )
                
            if isinstance(user_email, str):
                if len(user_email) < 4:
                    raise ValueError(
                        'ERROR in Users.update(). '
                        'user_email must be at least 4 letters long.'
                    )
                
