import psycopg
import logging
from scrapy.exceptions import CloseSpider
from datetime import datetime

default_logger = logging.getLogger(__name__)
class PostgresHandler(logging.Handler):

    def __init__(self, hostname,username,password,database, instance_name):
        super().__init__()

        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        self.instance_name = instance_name

        try:
            ## Create/Connect to database
            self.conn = psycopg.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database)
        except Exception as e:
            default_logger.error(f"Can't save LOGS. Error while connecting to Postgres at: {self.hostname}")
            raise CloseSpider(f"Can't save LOGS. Error while connecting to Postgres at: {self.hostname}")

        self.check_and_create_db()
        self.create_table()

    def check_and_create_db(self):
        cur = self.conn.cursor()
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{self.database}'")
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {self.database}")
            print(f"Database {self.database} created!")
        cur.close()

    def create_table(self):
        cur = self.conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS logs_table (
        id SERIAL PRIMARY KEY, 
        instance VARCHAR(40),
        level VARCHAR(25), 
        message TEXT, 
        ts timestamp
        );
        """)
        self.conn.commit()
        cur.close()

    def log_item(self, logger_level, item):
        cur = self.conn.cursor()

        cur.execute("""INSERT INTO logs_table (instance, level, message, ts) VALUES (%s, %s, %s, %s)""",
                    (self.instance_name, logger_level, item, datetime.now()))
        self.conn.commit()
        cur.close()

    def log_error(self, item):
        default_logger.error(f"Error:  {item}")
        self.log_item("ERROR", item)

    def log_info(self, item):
        self.log_item("INFO", item)

    def log_stat(self, item):
        self.log_item("STAT", item)