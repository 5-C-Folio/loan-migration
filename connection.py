from config import login
import cx_Oracle as db
db.init_oracle_client(lib_dir=r"C:\\oracle\\instantclient_12_1")


class DatabaseQuery():
    """run query and return dictonary results.  also headers"""
    def __init__(self, querystring='',
                user= login['user'],
                password = login["password"],
                dsn = login['dsn']
            ):
        self.user = user
        self.password = password
        self.dsn = dsn
        self.querystring = querystring
        self.headers = None

    @property
    def connection(self):
        """produce connection string.  Should make this inherit from connection object"""
        print("connecting")
        return db.connect( user = self.user,
                    password = self.password,
                    dsn = self.dsn)

    def search(self):
        """run query, store headers, make results dict"""
        cursor = self.connection.cursor()
        cursor.execute(self.querystring)
        print('executing query')
        self.headers = ([x[0] for x in cursor.description])
        cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description],args))
        print('returning results')
        return cursor.fetchall()


if __name__ == "__main__":
    print('hello')



