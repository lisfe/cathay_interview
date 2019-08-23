# -*- coding: utf-8 -*-
import scrapy
import pprint
import json
import logging
from rent_591.items import Rent591Item
from w3lib.html import remove_tags
import datetime

class Rent591CrawlerSpider(scrapy.Spider):
    # handle_httpstatus_list = [419]
    page_n = 30 # 30 records / page
    name = 'rent_591_crawler'
    allowed_domains = ['rent.591.com.tw']

    def start_requests(self):
        # first query
        yield scrapy.Request(
            'https://rent.591.com.tw/?kind=0&region=1',
            callback=self.handle_csrf
        )

    def handle_csrf(self, response):
        csrf = response.xpath('//head/meta[@name="csrf-token"]')[0].attrib['content']
        cities = {
            '台北市': 1,
            '新北市': 3
        }
        for city_name, city_num in cities.items():
            req = scrapy.Request(
                url=f'https://rent.591.com.tw/home/search/rsList?region={city_num}',
                headers={
                    'X-Requested-With': 'XMLHttpRequest',
                    'X-CSRF-TOKEN': csrf
                },
                cookies={'urlJumpIp': city_num},
                callback=self.handle_total)
            req.meta['csrf'] = csrf
            req.meta['city_name'] = city_name
            req.meta['city_num'] = city_num
            yield req

    def handle_total(self, response):
        data = json.loads(response.body_as_unicode())
        total_nums = int(data['records'].replace(',', ''))
        pages = total_nums // self.page_n + 1
        # print(pages)
        city_num = response.meta['city_num']
        city_name = response.meta['city_name']
        csrf = response.meta['csrf']
        for i in range(pages):
            req = scrapy.Request(
                url=f'https://rent.591.com.tw/home/search/rsList?region={city_num}&firstRow={i*30}',
                headers={
                    'X-Requested-With': 'XMLHttpRequest',
                    'X-CSRF-TOKEN': csrf
                },
                cookies={'urlJumpIp': city_num},
                callback=self.handle_basic_info)
            req.meta['csrf'] = csrf
            req.meta['city_name'] = city_name
            req.meta['city_num'] = city_num
            yield req

    def handle_basic_info(self, response: scrapy.http.response):
        data = json.loads(response.body_as_unicode())
        for info in data['data']['data']:
            # pprint.pprint(info)
            house_id = info['houseid']
            req = scrapy.Request(
                url=f'https://rent.591.com.tw/rent-detail-{house_id}.html',
                callback=self.handle_detail
            )
            req.meta['house_id'] = house_id
            req.meta['city_name'] = response.meta['city_name']
            req.meta['landlord'] = info['linkman']
            req.meta['landlord_sex'] = 1 if '先生' in info['linkman'] else 2
            req.meta['landlord_type'] = info['nick_name'].split()[0]
            req.meta['price'] = int(info['price'].replace(',', ''))
            # if 'all_sex' in info['condition']:
            #     req.meta['sex_limited'] = 0
            if 'boy' in info['condition']:
                req.meta['sex_limited'] = 1
            elif 'girl' in info['condition']:
                req.meta['sex_limited'] = 2
            else:
                req.meta['sex_limited'] = 0
            yield req

    def handle_detail(self, response):
        try:
            item = Rent591Item()
            item['fetch_datetime'] = datetime.datetime.utcnow()
            item['house_id'] = response.meta['house_id']
            item['city_name'] = response.meta['city_name']
            item['landlord'] = response.meta['landlord']
            item['landlord_type'] = response.meta['landlord_type']
            item['landlord_sex'] = response.meta['landlord_sex']
            item['price'] = response.meta['price']
            item['sex_limited'] = response.meta['sex_limited']
            item['phone_number'] = response.xpath('//span[@class="dialPhoneNum"]')[0].attrib['data-value']
            detail_info = response.xpath('//div[@class="detailInfo clearfix"]//li')
            # pprint.pprint(detail_info)
            for detail in detail_info:
                parsed_detail = remove_tags(''.join(detail.get().split())).split(':')
                # print(parsed_detail)
                if parsed_detail[0] == '型態':
                    item['house_type'] = parsed_detail[1]
                elif parsed_detail[0] == '現況':
                    item['house_status'] = parsed_detail[1]

            yield item

        except Exception as e:
            logging.warning(response.meta['house_id'])

