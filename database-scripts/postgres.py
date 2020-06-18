from pyspark.sql import DataFrameWriter


class PostgresConnector(object):
    def __init__(self, hostname, dbname, username, password):
        self.db_name = dbname
        self.hostname = hostname
        self.url_connect = "jdbc:postgresql://{hostname}:5432/{db}".format(
            hostname=self.hostname, db=self.db_name)
        self.properties = {
            "user": username,
            "password": password,
            "driver": "org.postgresql.Driver"
        }

    def write(self, df, table_name, mode):
        """
        Args:
            df: spark.dataframe
            table_name: str
            moded: str 
        :rtype: None
        """
        writer = DataFrameWriter(df)
        writer.jdbc(self.url_connect, table_name, mode, self.properties)
