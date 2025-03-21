from google_cloud import CloudSQLConnection

with CloudSQLConnection() as db:
    db.cursor.execute('SELECT * FROM Users WHERE user_id = 1;')
    result = db.cursor.fetchone()

print(result)

