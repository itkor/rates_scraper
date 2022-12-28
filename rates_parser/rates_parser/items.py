# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class RatesItem(scrapy.Item):
    operation_category = scrapy.Field() #(exchange offices, smartbank ... )
    operation_type = scrapy.Field() # (buy / sell)
    currency = scrapy.Field() # USD, EUR
    currency_description = scrapy.Field() # U.S. dollars
    rate = scrapy.Field() # The rate value
    timestamp = scrapy.Field()
