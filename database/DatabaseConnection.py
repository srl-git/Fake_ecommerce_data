import sqlite3

class DatabaseConnection:

    def __init__(self, path: str) -> None:

        if not (isinstance(path, str) and path.endswith('.db')):
            raise ValueError('The path to the database file must be a non empty string with a .db file extension')

        self.path = path

    def __enter__(self):

        self.connection = sqlite3.connect(self.path)
        self.cursor = self.connection.cursor()

        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        
        try:
            self.connection.commit()
    
        except sqlite3.Error as e:
            self.connection.rollback()
            print(f'Database transaction failed and rolled back: {e}')

        finally:
            if self.connection:
                self.connection.close()
