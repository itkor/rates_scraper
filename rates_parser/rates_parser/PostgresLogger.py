import psycopg
import logging
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import CloseSpider
from datetime import datetime

default_logger = logging.getLogger(__name__)
class PostgresHandler(logging.Handler):

    def __init__(self, hostname,username,password,database, connection):
        super().__init__()
        # settings = get_project_settings()
        # self.hostname = settings.get('PG_HOSTNAME')
        # self.username = settings.get('PG_USERNAME')
        # self.password = settings.get('PG_PASS')
        # self.database = settings.get('PG_LOGS_DB')
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        #
        # default_logger.error(f"Hostname: {self.hostname}")
        # default_logger.error(f"username: {self.username}")
        # default_logger.error(f"password: {self.password}")
        # default_logger.error(f"database: {self.database}")
        #
        # self.conn_dict = {
        #     "host" : self.hostname,
        #     "user" : self.username,
        #     "password" : self.password,
        #     "dbname" : self.database
        # }
        default_logger.error(f"Custom Logger initiated with  {connection}")
        self.conn = connection

        # Use the same connection to access different databases
        # self.cur = self.conn.cursor()
        # self.cur.execute(f"SET search_path TO {self.database}")

        # try:
        #     ## Create/Connect to database
        #     self.conn = psycopg.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database)
        # except Exception as e:
        #     default_logger.error(f"Can't save LOGS. Error while connecting to Postgres at: {self.hostname}")
        # raise CloseSpider(f"Can't save LOGS. Error while connecting to Postgres at: {self.hostname}")

        # self.check_and_create_db()
        self.create_table()

    def check_and_create_db(self):
        cur = self.conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname='logs'")
        if not cur.fetchone():
            cur.execute("CREATE DATABASE logs")
            print("Database logs created!")
        cur.close()
        # self.conn.close()
        # self.conn = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database)

    def create_table(self):
        cur = self.conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS logs_table (
        id SERIAL PRIMARY KEY, 
        logger VARCHAR(25),
        level VARCHAR(25), 
        message TEXT, 
        ts timestamp
        );
        """)
        self.conn.commit()
        cur.close()

    def log_item(self, logger_name, logger_level, item):
        # cur = self.conn.cursor()
        # Use the same connection to access different databases
        cur = self.conn.cursor()
        # cur.execute(f"SET search_path TO {self.database}")
        default_logger.error(f"Logging item  {item}")
        cur.execute("""INSERT INTO logs_table (logger, level, message, ts) VALUES (%s, %s, %s, %s)""",
                    (logger_name, logger_level, item, datetime.now()))
        self.conn.commit()
        cur.close()

    def log_error(self, item):
        default_logger.error(f"Log_error item  {item}")
        self.log_item('ERROR_LOGGER', "ERROR", item)

    def log_info(self, item):
        self.log_item('INFO_LOGGER', "INFO", item)

    def log_stat(self, item):
        self.log_item('STAT_LOGGER', "STAT", item)