from __future__ import absolute_import
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
from rates_parser.items import RatesItem
import scrapy
import logging
from datetime import datetime, timezone

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError


def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

# Setting up logging =============================================
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
# info logger
info_logger = setup_logger('info_logger', 'ratescrawler_info.log')
# error  logger
error_logger = setup_logger('error_logger', 'ratescrawler_error.log')
# stat logger
stat_logger = setup_logger('stat_logger', 'ratescrawler_stat.log')

class RatesSpider(scrapy.Spider):
    name = "ratescrawler"
    #header = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"}
    allowed_domains = ['eubank.kz']

    def start_requests(self):
        url = 'https://eubank.kz/exchange-rates/?lang=en'
        info_logger.info(f'Parser started')
        yield scrapy.Request(url=url, callback=self.parse)


    def __init__(self):

        self.cookies = {"TAUD":"LA-1604943341832-1*RDD-1-2020_11_09*RD-264330280-2020_11_12.21142557*HDD-3018576153-2020_12_27.2020_12_28*LG-3107446580-2.1.F.*LD-3107446581-.....*CUR-1-EUR",
                        "TALanguage":"en"
                        }
        self.location_cnt = 0
        self.num_rests = 0
        self.reviews_cnt = 0
        self.basic_url = "https://www.tripadvisor.com"
        self.current_location_name = ''
        self.current_location_url = ''

    def parse(self, response):
        '''
        exchange_col contains
            exchange_title, exchange_description, exchange_table value

        There are 8 exchange_col's: 2 cols in each "operation_category"
        ("At exchange offices","In smartbank","For legal entities","Gold bars")
        '''
        # self.logger.info('Parse function called on %s', response.url)
        self.logger.info('User-Agent:  %s', response.request.headers['User-Agent'])

        rate_record_item = RatesItem()

        for i in range(1,9):

            # Get list values (exchange_title, exchange_description, exchange_table value)
            try:
                rates_lst = response.xpath(f"(//table[@class='exchange-table'])[{i}]//descendant::span/text()").getall()
            except Exception as e:
                error_logger.error(f"Error in the path for exchange-table")

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
            if i // 2 == 0:
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
        self.logger.error(repr(failure))
        error_logger.error(repr(failure))

        #if isinstance(failure.value, HttpError):
        if failure.check(HttpError):
            # you can get the response
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)
            error_logger.error('HttpError on %s', response.url)

        #elif isinstance(failure.value, DNSLookupError):
        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)
            error_logger.error('DNSLookupError on %s', request.url)

        #elif isinstance(failure.value, TimeoutError):
        elif failure.check(TimeoutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)
            error_logger.error('TimeoutError on %s', request.url)