# -*- coding: utf-8 -*-
# import os
import scrapy
# import pdb
import datetime, time
import re
import json
from lxml import etree
import requests
import pymysql.cursors


# 需要更改的设置位置
debugging = False    # 若测试则开启抓取结束比赛，并由detect_date指定日期
detect_date = ['2018-01-03']
# 赛季文本 2017/2018
seasonText = '2017/2018'

competition_list = ['8','16','9','13','1','43','7','70','284','430','135','177','18','10','537','1417','150','283',
                    '15','32','102','640','45','87','107','14','5','12','88','136','11','26','89','155','31','63',
                    '162','121','122','109','110','29','36','91','61','17','24','119','216','519','22','82',
                    '33','85','19','97','73','27','28','34','51','138','93']   # 需要获取信息的联赛competition_id list

current_hour = time.localtime()[3]  # 获取当前的小时数，如果小于8则应该选择yesterday
nowadays = datetime.datetime.now().strftime("%Y-%m-%d")  # 获取当前日期 格式2018-01-01
yesterdy = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")  # 获取昨天日期
if current_hour < 8:    # 默认在早上八点前拉取昨天页面
    search_date = yesterdy
else:
    search_date = nowadays

if debugging:
    include_end = True   # 开启则会读取所有当日比赛，否则只会读取未结束的比赛 默认应该为 False
    search_date = detect_date
else:
    include_end = False
# 需要更改的设置结束

# 根据data_competition获取联赛名称
data_competition_dict = {
    '8': '英超',
    '93': '英足总杯',
    '16': '法甲',
    '9': '德甲',
    '11': '德乙',
    '13': '意甲',
    '1': '荷甲',
    '5': '荷乙',
    '43': '苏超',
    '7': '西甲',
    '12': '西乙',
    '70': '英冠',
    '284': '世俱杯',
    '430': '友谊赛',
    '135': '意大利杯',
    '14': '意乙',
    '177': '法国杯',
    '17': '法乙',
    '18': '欧罗巴',
    '10': '欧冠',
    '537': '孟加拉超',
    '1417': '印超',
    '150': '印甲',
    '283': '澳超',
    '74': '威尔士超',
    '15': '英甲',
    '32': '英乙',
    '24': '比甲',
    '52': '比乙',
    '63': '葡超',
    '119': '波兰超',
    '102': '葡萄牙杯',
    '640': '葡萄牙联赛杯',
    '107': '希腊超',
    '45': '苏冠',
    '46': '苏乙',
    '87': '阿甲',
    '88': '阿乙',
    '26': '巴甲',
    '89': '巴乙',
    '155': '墨甲',
    '31': '冰岛超',
    '29': '挪超',
    '36': '挪甲',
    '22': '芬超',
    '117': '伊朗超',
    '76': '伊朗甲',
    '136': '韩K联',
    '162': '乌拉圭甲',
    '61': '克罗地亚甲',
    '82': '捷甲',
    '121': '俄超',
    '122': '俄甲',
    '109': '日职联',
    '110': '日乙',
    '577': '天皇杯',
    '91': '哥伦比亚甲',
    '216': '沙特职',
    '519': '泰超',
    '33': '美职',
    '85': '罗马利亚甲',
    '19': '土超',
    '97': '土甲',
    '73': '新西兰超',
    '27': '瑞士超',
    '28': '瑞典超',
    '34': '爱超',
    '51': '中超',
    '138': '西班牙国王杯'
}

match_name_dict = {
    'Australia - A-League': '澳超',
    'India - Indian Super League': '印度超',
    'Portugal - Primeira Liga': '葡超'
}

completed_match_id_list = []

# 单场比赛 item
class match_Item(scrapy.Item):
    match_name = scrapy.Field()  # 联赛名称
    has_analysed = scrapy.Field()  # 是否已经分析过
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
    name = 'auto_teams_rate'
    allowed_domains = ['https://cn.soccerway.com/']

    if not debugging:
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
                # 建立当前队伍表
                build_table = (
                    "CREATE TABLE IF NOT EXISTS "' %s '""
                    "(match_id VARCHAR(20) NOT NULL PRIMARY KEY,"
                    "match_name VARCHAR(50) NOT NULL,"
                    "home_name VARCHAR(50) NOT NULL,"
                    "away_name VARCHAR(50) NOT NULL,"
                    "time_score VARCHAR(50) NOT NULL,"
                    "home_rate FLOAT(8) NOT NULL,"
                    "home_a_value INT(10) NOT NULL,"
                    "away_rate FLOAT(8) NOT NULL,"
                    "away_a_value INT(10) NOT NULL,"
                    "average_completed_match INT(8) NOT NULL,"
                    "home_direction_probability FLOAT(8) NOT NULL,"
                    "away_direction_probability FLOAT(8) NOT NULL,"
                    "support_direction VARCHAR(30) NOT NULL,"
                    "support_direction_2 VARCHAR(30) NOT NULL)"
                )
                cursor.execute(build_table % tableName)
                # 建表完成

                cursor.execute('SELECT match_id FROM %s WHERE home_rate>0 and away_rate>0' % tableName)
                for matchId in cursor.fetchall():
                    completed_match_id_list.append(matchId['match_id'])
                # connection is not autocommit by default. So you must commit to save your changes.
                cursor.close()
        finally:
            connection.close()
        print("已经获得首发的比赛ID列表:", completed_match_id_list)

    # 包装url
    start_urls = []
    if debugging:
        for day in search_date:
            url = 'https://cn.soccerway.com/matches/' + day.replace('-', '/') + '/'
            start_urls.append(url)
    else:
        url = 'https://cn.soccerway.com/matches/' + search_date.replace('-', '/') + '/'
        start_urls = [url]
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url)

    # 分析每行信息
    def parse(self, response):
        current_search_date = response.url.split('/')[-4] + '-' + response.url.split('/')[-3] + '-' + response.url.split('/')[-2]
        # 已经展开的一流比赛
        for tr in response.xpath('//table').xpath('//tr[contains(@class,"match")]'):
            data_competition = tr.xpath('@data-competition').extract()[0]
            match_id = tr.xpath('@id').extract()[0].split('-')[-1]  # 比赛ID
            # 实用模式不再拉取已经分析首发率的比赛球员信息
            if not debugging and match_id in completed_match_id_list:
                has_analysed = True
            else:
                has_analysed = False
            match_name = data_competition_dict[data_competition]    # 根据字典对应比赛中文名称
            state_text = tr.xpath('td')[0].xpath('text()').extract()[0].strip()     # 如果已经结束则会显示 全时
            # if not include_end and state_text == '全时':
            #     continue
            home_name = tr.xpath('td')[1].xpath('a/text()').extract()[0].strip()
            away_name = tr.xpath('td')[3].xpath('a/text()').extract()[0].strip()
            href = 'https://cn.soccerway.com' + tr.xpath('td')[2].xpath('a/@href').extract()[0].strip()
            yield scrapy.Request(href, meta={'match_id':match_id,'match_name':match_name,'home_name':home_name, 'away_name':away_name, 'main_match':True, 'has_analysed':has_analysed}, callback=self.single_match_parse, dont_filter = True)
        # 未展开的比赛头
        for tr in response.xpath('//table').xpath('//tr[contains(@class,"clickable ")]'):
            stageValue = tr.xpath('@stage-value').extract()[0]  # stage-value
            competition_id = tr.xpath('@id').extract()[0].split('-')[1] # competition_id
            if competition_id not in competition_list:
                continue
            match_name = tr.xpath('th/h3/span/text()').extract()[0] # 比赛名称（某个联赛或杯赛等）
            if match_name in match_name_dict.keys():
                match_name = match_name_dict[match_name]    # 中英文转换
            href = 'https://cn.soccerway.com/a/block_date_matches?block_id=page_matches_1_block_date_matches_1&callback_params=%7B"bookmaker_urls"%3A%5B%5D%2C"block_service_id"%3A"matches_index_block_datematches"%2C"date"%3A"'+current_search_date+'"%2C"stage-value"%3A"'+stageValue+'"%7D&action=showMatches&params=%7B"competition_id"%3A'+competition_id+'%7D'
            yield scrapy.Request(href, meta={'match_name': match_name},callback=self.sub_matchs_parse, dont_filter=True)


    # 获取单场比赛具体信息
    # 在这获取首发名单
    def single_match_parse(self, response):
        handle_httpstatus_list = [404]
        if response.status in handle_httpstatus_list:
            print('访问404')
            return False
        main_match = response.meta['main_match']    # 一流比赛为True
        has_analysed = response.meta['has_analysed']
        match_id = response.meta['match_id']
        match_name = response.meta['match_name']
        home_name = response.meta['home_name']
        away_name = response.meta['away_name']
        home_player_shirtNumber_list = []
        away_player_shirtNumber_list = []
        # time_score有时候在span下，有时候没有span
        temp_time_score = response.xpath('//h3[contains(@class,"scoretime")]/text()').extract()[0].strip()
        if temp_time_score == '':
            time_score = response.xpath('//h3[contains(@class,"scoretime")]/span/text()').extract()[0].strip()  # 得到 未开赛的时间，开赛的比分   # 获取的是东一区时间
        else:
            time_score = temp_time_score

        # 与当前时间比较 因为获取的是东一区时间所以要+7小时
        time_split_list = time_score.split(':')
        if len(time_split_list) > 1:
            start_hour = int(time_split_list[0]) + 7
            cur_hour = time.localtime()[3]
            if cur_hour < 8:
                cur_hour += 24
            # 如果开赛小时比当前多1或者<3就不往下读取信息
            if (start_hour - cur_hour) > 1 or (start_hour - cur_hour) < -3:
                return False

        average_completed_match = 0     # 平均比赛轮次
        # 没有获得首发球员的比赛才去拉取球员信息
        if not has_analysed:
            lineups_container = response.xpath('//div[contains(@class,"combined-lineups-container")]')  # 如果首发已出则长度不为0，否则存在首发
            if len(lineups_container) == 0:
                print('首发未出')
                home_firstEleven_rate = 0
                away_firstEleven_rate = 0
                support_direction = 0
            else:
                home_completed_match = ''
                away_completed_match = ''
                # 先遍历积分榜highlight，找出当前主，客已比赛轮数
                for highlight_tr in response.xpath('//table[contains(@class,"leaguetable")]/tbody/tr[contains(@class,"highlight")]'):
                    team_name = highlight_tr.xpath('td')[1].xpath('a/text()').extract()[0]
                    if team_name == home_name:
                        home_completed_match = int(highlight_tr.xpath('td')[2].xpath('text()').extract()[0])
                    else:
                        away_completed_match = int(highlight_tr.xpath('td')[2].xpath('text()').extract()[0])
                    if home_completed_match != '' and away_completed_match != '':
                        break
                average_completed_match = (home_completed_match + away_completed_match)/2

                # 开始收集主客球员首发信息
                # home_team
                home_tr_count = 0
                home_firstElevenNum = 0
                for tr in lineups_container[0].xpath('div/table')[0].xpath('tbody/tr'):    # 循环主队首发tr，最后一个是主教练要排除, 所以只选择0-10
                    if home_tr_count == 11:
                        break
                    try:
                        player_href = 'https://cn.soccerway.com' + tr.xpath('td')[-2].xpath('a/@href').extract()[0]
                    except:
                        print('error:298')
                    player_shirtNumber = tr.xpath('td')[0].xpath('text()').extract()[0]
                    home_player_shirtNumber_list.append(player_shirtNumber)
                    player_page = ''
                    while player_page == '':
                        try:
                            player_page = requests.get(player_href)
                        except:
                            print("Connection refused by the server..")
                            print("Let me sleep for 5 seconds")
                            print("ZZzzzz...")
                            time.sleep(5)
                            print("Was a nice sleep, now let me continue...")
                            continue
                    page = etree.HTML(player_page.text)
                    for seasonTr in page.xpath('//table[contains(@class,"playerstats")]/tbody/tr'):     # 循环球员的赛季表现tr找出满足本赛季本球队的首发信息
                        if seasonTr.xpath('td')[0].xpath('a/text()')[0] == seasonText and seasonTr.xpath('td')[1].xpath('a/text()')[0] == home_name:
                            firstEleven_num_text = seasonTr.xpath('td')[5].xpath('text()')[0]
                            if firstEleven_num_text.isdigit():
                                home_firstElevenNum += int(firstEleven_num_text)  # 首发次数
                            else:
                                home_firstElevenNum += 0
                            break
                    home_tr_count += 1

                # away_team
                away_tr_count = 0
                away_firstElevenNum = 0
                for tr in lineups_container[0].xpath('div/table')[1].xpath('tbody/tr'):  # 循环客队首发tr，最后一个是主教练要排除, 所以只选择0-10
                    if away_tr_count == 11:
                        break
                    player_href = 'https://cn.soccerway.com' + tr.xpath('td')[-2].xpath('a/@href').extract()[0]
                    player_shirtNumber = tr.xpath('td')[0].xpath('text()').extract()[0]
                    away_player_shirtNumber_list.append(player_shirtNumber)
                    player_page = ''
                    while player_page == '':
                        try:
                            player_page = requests.get(player_href)
                        except:
                            print("Connection refused by the server..")
                            print("Let me sleep for 5 seconds")
                            print("ZZzzzz...")
                            time.sleep(5)
                            print("Was a nice sleep, now let me continue...")
                            continue
                    page = etree.HTML(player_page.text)
                    for seasonTr in page.xpath('//table[contains(@class,"playerstats")]/tbody/tr'):  # 循环球员的赛季表现tr找出满足本赛季本球队的首发信息
                        if seasonTr.xpath('td')[0].xpath('a/text()')[0] == seasonText and seasonTr.xpath('td')[1].xpath('a/text()')[0] == away_name:
                            firstEleven_num_text = seasonTr.xpath('td')[5].xpath('text()')[0]
                            if firstEleven_num_text.isdigit():
                                away_firstElevenNum += int(firstEleven_num_text)  # 首发次数
                            else:
                                away_firstElevenNum += 0
                            break
                    away_tr_count += 1

                # 开始计算首发率
                support_direction = 0
                home_firstEleven_rate = round(home_firstElevenNum/11/home_completed_match, 2)
                away_firstEleven_rate = round(away_firstElevenNum/11/away_completed_match, 2)
                rate_gap = round(abs(home_firstEleven_rate - away_firstEleven_rate), 2)

                # # 防止大于1的错误数据影响
                first_limit_gap = 0.22  # limit_gap值
                if home_firstEleven_rate < 1 and away_firstEleven_rate < 1:
                    if rate_gap >= first_limit_gap:
                        if home_firstEleven_rate > away_firstEleven_rate:
                            support_direction = 1
                        else:
                            support_direction = -1
                    # if away_firstEleven_rate >= 0.75:
                    #     support_direction = -0.5
        # 已经获取首发了的比赛，pipeline中要判断has_analysed不update 下面else中几种数据信息
        else:
            home_firstEleven_rate = ''
            away_firstEleven_rate = ''
            support_direction = ''

        # 将数据保存到数据模型中
        single_match_Item = match_Item()
        single_match_Item['has_analysed'] = has_analysed  # 联赛名称
        single_match_Item['match_id'] = match_id  # 联赛名称
        single_match_Item['match_name'] = match_name  # 联赛名称
        single_match_Item['home_name'] = home_name  # 主队名称
        single_match_Item['away_name'] = away_name  # 客队名称
        single_match_Item['time_score'] = time_score  # 比赛时间或者比分
        single_match_Item['home_rate'] = home_firstEleven_rate  # 主队首发率
        single_match_Item['away_rate'] = away_firstEleven_rate  # 客队首发率
        single_match_Item['average_completed_match'] = average_completed_match  # 平均首发轮次
        single_match_Item['home_player_shirtNumber_list'] = home_player_shirtNumber_list  # 主队首发number list
        single_match_Item['away_player_shirtNumber_list'] = away_player_shirtNumber_list  # 客队首发number list
        single_match_Item['support_direction'] = support_direction  # 首发率支持方向
        yield single_match_Item

    # 获取次级比赛列表信息
    def sub_matchs_parse(self, response):
        handle_httpstatus_list = [404]
        if response.status in handle_httpstatus_list:
            print('访问404')
            return False
        matchs_info = json.loads(response.body)
        page = etree.HTML(matchs_info['commands'][0]['parameters']['content'].strip())
        match_name = response.meta['match_name']    # 联赛名称
        for tr in page.xpath('//tr'):
            if len(tr.xpath('td')) == 0:
                continue
            state_text = tr.xpath('td')[0].xpath('text()')[0].strip()  # 如果已经结束则会显示 全时
            # if not include_end and state_text == '全时':
            #     continue
            try:
                match_id = tr.xpath('@id')[0].split('-')[-1]  # 比赛ID
                # 实用模式不再拉取已经分析首发率的比赛球员信息
                if not debugging and match_id in completed_match_id_list:
                    has_analysed = True
                else:
                    has_analysed = False
                home_name = tr.xpath('td')[1].xpath('a')[0].attrib['title']
                away_name = tr.xpath('td')[3].xpath('a')[0].attrib['title']
                href = 'https://cn.soccerway.com' + tr.xpath('td')[2].xpath('a')[0].attrib['href']
            except Exception as e:
                p
            yield scrapy.Request(href, meta={'match_id':match_id,'match_name': match_name,'home_name':home_name, 'away_name':away_name, 'main_match':False, 'has_analysed':has_analysed}, callback=self.single_match_parse, dont_filter=True)









