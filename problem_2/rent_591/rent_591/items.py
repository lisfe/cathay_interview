# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Rent591Item(scrapy.Item):
    house_id = scrapy.Field()
    city_name = scrapy.Field()
    landlord = scrapy.Field()
    landlord_type = scrapy.Field()
    landlord_sex = scrapy.Field()
    price = scrapy.Field()
    phone_number = scrapy.Field()
    sex_limited = scrapy.Field()
    house_type = scrapy.Field()
    house_status = scrapy.Field()
    fetch_datetime = scrapy.Field()
