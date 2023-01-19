from __future__ import absolute_import
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
from rates_parser.items import RatesItem
import scrapy
from datetime import datetime, timezone
from rates_parser.PostgresLogger import PostgresHandler
import logging
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import CloseSpider


# Default logger for looging in cases when custom logger is unavailable
default_logger = logging.getLogger(__name__)
class RatesSpider(scrapy.Spider):
    name = "ratescrawler"
    header = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"}
    allowed_domains = ['eubank.kz']

    def start_requests(self):
        url = 'https://eubank.kz/exchange-rates/?lang=en'
        yield scrapy.Request(url=url, callback=self.parse)

    def __init__(self, *args, **kwargs):
        # Access project settings for environment variables
        self.settings=get_project_settings()
        super(RatesSpider, self).__init__(*args, **kwargs)

        # Initiate logger and get settings for the Postgres connection
        hostname = self.settings.get("PG_HOSTNAME")
        username = self.settings.get('PG_USERNAME')
        password = self.settings.get('PG_PASS')
        log_database = self.settings.get('PG_LOGS_DB')
        instance_name = 'rates_spider'
        try:
            self.cust_logger = PostgresHandler(hostname,username,password,log_database, instance_name)
        except Exception as e:
            default_logger.error(f"Error initializing CustomLogger to Postgres in {instance_name}")
            raise CloseSpider(f"Error initializing CustomLogger to Postgres in {instance_name}")
    def parse(self, response):
        '''
        exchange_col contains
            exchange_title, exchange_description, exchange_table value

        There are 8 exchange_col's: 2 cols in each "operation_category"
        ("At exchange offices","In smartbank","For legal entities","Gold bars")
        '''


        rate_record_item = RatesItem()

        for i in range(1,9):

            # Get list values (exchange_title, exchange_description, exchange_table value)
            try:
                rates_lst = response.xpath(f"(//table[@class='exchange-table'])[{i}]//descendant::span/text()").getall()
            except Exception as e:
                self.cust_logger.log_error(f"Error in the path for exchange-table")

            # Getting timestamp for the data
            timestamp = datetime.now(timezone.utc)

            operation_category = ''
            if i in [1,2]:
                operation_category = 'At exchange offices'
            elif i in [3,4]:
                operation_category = 'In Smartbank'
            elif i in [5,6]:
                operation_category = 'For legal entities'
            elif i in [7,8]:
                operation_category = 'Gold bars'

            operation_type = ''
            # operation_type choice
            if i % 2 == 1:
                operation_type = 'Sell'
            else:
                operation_type = 'Buy'

            rates_composite_list = [rates_lst[x:x+3] for x in range(0, len(rates_lst),3)]
            for currency_record in rates_composite_list:
                rate_record_item['operation_category'] = operation_category
                rate_record_item['operation_type'] = operation_type
                rate_record_item['currency'] = currency_record[0]
                rate_record_item['currency_description'] = currency_record[1]
                rate_record_item['rate'] = currency_record[2]

                rate_record_item['timestamp']=timestamp

                yield rate_record_item

    def errback_httpbin(self, failure):
        # log all errback failures,
        # in case you want to do something special for some errors,
        # you may need the failure's type
        self.cust_logger.log_error(repr(failure))

        #if isinstance(failure.value, HttpError):
        if failure.check(HttpError):
            # you can get the response
            response = failure.value.response
            self.cust_logger.log_error(f'HttpError on {response.url}')


        #elif isinstance(failure.value, DNSLookupError):
        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            self.cust_logger.log_error(f'DNSLookupError on {request.url}')


        #elif isinstance(failure.value, TimeoutError):
        elif failure.check(TimeoutError):
            request = failure.request
            self.cust_logger.log_error(f'TimeoutError on {request.url}')