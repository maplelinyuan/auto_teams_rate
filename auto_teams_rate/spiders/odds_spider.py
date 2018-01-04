# -*- coding: utf-8 -*-
# import os
import scrapy
import pdb
import datetime, time
import re
import json
from lxml import etree
import requests
import pymysql.cursors


match_name_dict = {
    'ISL': '印度超',
    'EGY D1': '埃及超',
    'INT CF': '国际友谊',
    'SPA CUP': '西杯',
    'POR D1': '葡超',
    'ENG PR': '英超',
    'AUS D1': '澳超',
}

current_hour = time.localtime()[3]  # 获取当前的小时数，如果小于8则应该选择yesterday
nowadays = datetime.datetime.now().strftime("%Y-%m-%d")  # 获取当前日期 格式2018-01-01
yesterdy = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")  # 获取昨天日期
if current_hour < 8:    # 默认在早上八点前拉取昨天页面
    search_date = yesterdy
else:
    search_date = nowadays

completed_match_id_list = []
# Connect to the database
# 去拿取已经获得首发率的match_id放入列表中，不去服务器拉取该比赛球员数据
db_name = 'auto_teams_rate'
config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '19940929',
    'db': db_name,
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}
connection = pymysql.connect(**config)
print('连接至数据库:' + db_name)
try:
    with connection.cursor() as cursor:
        # 设置当前表名
        tableName = 'teams_' + search_date.replace('-', '_')  # 当前查询日期为表名
        cursor.execute('SELECT * FROM %s WHERE home_rate>0 and away_rate>0' % tableName)
        for match in cursor.fetchall():
            single_match = {}
            single_match['id'] = match['match_id']
            single_match['match_name'] = match['match_name']
            single_match['time_score'] = match['time_score']
            single_match['home_rate'] = match['home_rate']
            single_match['away_rate'] = match['away_rate']
            completed_match_id_list.append(single_match)
        # connection is not autocommit by default. So you must commit to save your changes.
        cursor.close()
finally:
    connection.close()
print("已经获得首发的比赛ID列表:",completed_match_id_list)

# 单场比赛 item
class match_Item(scrapy.Item):
    match_name = scrapy.Field()  # 联赛名称
    has_analysed = scrapy.Field()  # 比赛ID
    match_id = scrapy.Field()  # 比赛ID
    home_name = scrapy.Field()  # 主队名称
    away_name = scrapy.Field()    # 客队名称
    time_score = scrapy.Field()   # 比赛时间或者结果
    home_rate = scrapy.Field()   # 主队首发率
    away_rate = scrapy.Field()   # 客队首发率
    average_completed_match = scrapy.Field()   # 平均首发轮次
    home_player_shirtNumber_list = scrapy.Field()   # 主队首发球员shirtNumber列表
    away_player_shirtNumber_list = scrapy.Field()   # 客队首发球员shirtNumber列表
    support_direction = scrapy.Field()  # 支持方向

class SoccerSpider(scrapy.Spider):
    name = 'odds_spider'
    allowed_domains = ['http://info.livescore123.com/']

    # 包装url
    start_urls = []
    url = 'http://info.livescore123.com/1x2/company.aspx?id=177&company=PinnacleSports'
    start_urls.append(url)

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url)

    # 分析每行信息
    def parse(self, response):
        count = 0
        odd_match_list = []
        for tr in response.xpath('//table[contains(@class,"schedule")]').xpath('tr'):
            if count == 0:
                continue
            if count % 2 == 1:
                league_name = tr.xpath('td')[0].xpath('text()').extract()[0]    # 联赛名称，是英文，需要用字典转为中文
                start_time_year = int(tr.xpath('td')[1].xpath('script/text()').extract()[0].replace('showtime(', '').replace(')', '').split(',')[0])
                start_time_month = int(tr.xpath('td')[1].xpath('script/text()').extract()[0].replace('showtime(', '').replace(')', '').split(',')[1].split('-')[0])
                start_time_day = int(tr.xpath('td')[1].xpath('script/text()').extract()[0].replace('showtime(', '').replace(')', '').split(',')[2])
                start_time_hour = int(tr.xpath('td')[1].xpath('script/text()').extract()[0].replace('showtime(', '').replace(')', '').split(',')[3])
                start_time_minu = int(tr.xpath('td')[1].xpath('script/text()').extract()[0].replace('showtime(', '').replace(')', '').split(',')[4])
                start_time = datetime.datetime(start_time_year, start_time_month, start_time_day, start_time_hour, start_time_minu) + datetime.timedelta(hours=8)
                start_time_text = start_time.strftime('%Y-%m-%d %H:%M')
                home_name = tr.xpath('td')[2].xpath('a/text()').extract()[0]
                away_name = tr.xpath('td')[7].xpath('a/text()').extract()[0]
                home_original_odd = tr.xpath('td')[3].xpath('text()').extract()[0]
                draw_original_odd = tr.xpath('td')[4].xpath('text()').extract()[0]
                away_original_odd = tr.xpath('td')[5].xpath('text()').extract()[0]
                home_original_probability = tr.xpath('td')[6].xpath('text()').extract()[0]
                draw_original_probability = tr.xpath('td')[7].xpath('text()').extract()[0]
                away_original_probability = tr.xpath('td')[8].xpath('text()').extract()[0]
                original_payBack_rate = tr.xpath('td')[9].xpath('text()').extract()[0]
                single_match_dict = {}
                single_match_dict['league_name'] = match_name_dict[league_name]
                single_match_dict['start_time_text'] = start_time_text
                single_match_dict['home_name'] = home_name
                single_match_dict['away_name'] = away_name
                single_match_dict['home_original_odd'] = home_original_odd
                single_match_dict['draw_original_odd'] = draw_original_odd
                single_match_dict['away_original_odd'] = away_original_odd
                single_match_dict['home_original_probability'] = home_original_probability
                single_match_dict['draw_original_probability'] = draw_original_probability
                single_match_dict['away_original_probability'] = away_original_probability
                single_match_dict['original_payBack_rate'] = original_payBack_rate
                odd_match_list.append(single_match_dict)
            else:
                match_index = int(count/2)
                home_now_odd = tr.xpath('td')[0].xpath('text()').extract()[0]
                draw_now_odd = tr.xpath('td')[1].xpath('text()').extract()[0]
                away_now_odd = tr.xpath('td')[2].xpath('text()').extract()[0]
                home_now_probability = tr.xpath('td')[3].xpath('text()').extract()[0]
                draw_now_probability = tr.xpath('td')[4].xpath('text()').extract()[0]
                away_now_probability = tr.xpath('td')[5].xpath('text()').extract()[0]
                now_payBack_rate = tr.xpath('td')[6].xpath('text()').extract()[0]
                odd_match_list[match_index]['home_now_odd'] = home_now_odd
                odd_match_list[match_index]['draw_now_odd'] = draw_now_odd
                odd_match_list[match_index]['away_now_odd'] = away_now_odd
                odd_match_list[match_index]['home_now_probability'] = home_now_probability
                odd_match_list[match_index]['draw_now_probability'] = draw_now_probability
                odd_match_list[match_index]['away_now_probability'] = away_now_probability
                odd_match_list[match_index]['now_payBack_rate'] = now_payBack_rate
            count += 1


            # yield scrapy.Request(href, meta={'match_id':match_id}, callback=self.single_match_parse, dont_filter = True)











