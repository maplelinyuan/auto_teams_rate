# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

# 需要更改的参数
current_season = '17t18'

import pymysql.cursors
import datetime
import json
import pdb

nowadays = datetime.datetime.now().strftime('%Y_%m_%d')
nowatime = datetime.datetime.now().strftime('%Y_%m_%d_%H%M')

class AutoTeamsRatePipeline(object):
    def process_item(self, item, spider):
        if spider.name == 'nauto_teams_rateews':
        # 这里写爬虫 auto_teams_rate 的逻辑
            # 获取查询日期
            search_date = spider.url.split('/')[-4] + '_' + spider.url.split('/')[-3] + '_' + spider.url.split('/')[-2]
            # Connect to the database
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
            print('连接至数据库' + db_name)
            try:
                with connection.cursor() as cursor:
                    # 设置当前表名
                    tableName = 'teams_' + search_date  # 表名
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

                    home_a_value = 0
                    away_a_value = 0
                    first_direction = item['support_direction']    # 考虑首发率的支持方向
                    second_direction = 0    # 根据市场价与首发率一起考虑的支持方向
                    # 根据主客名称去查询身价，并计算出平均身价
                    with open('auto_teams_rate/league2english.json', 'r', encoding='utf-8') as league_json_file:
                        league2english = json.load(league_json_file)
                    league2english_key_list = league2english.keys()
                    with open('auto_teams_rate/chinese2english.json', 'r', encoding='utf-8') as json_file:
                        chinese2english = json.load(json_file)
                    home_direction_probability = 0
                    away_direction_probability = 0
                    # 如果已经设置了本联赛的中英转换 且 主客任一shirtNumber_list 不为空就继续
                    if (item['match_name'] in league2english_key_list or (item['home_name'] in chinese2english.keys() and item['away_name'] in chinese2english.keys())) and len(item['home_player_shirtNumber_list']) != 0:
                        home_english_name = chinese2english[item['home_name']]['name']
                        home_database_name = current_season + '_league_' + chinese2english[item['home_name']]['league']
                        # 搜索主队首发的身价
                        home_table_name = home_database_name + '.teams_' + home_english_name
                        temp_home_shirtNum_purple = '('
                        for shirtNum in item['home_player_shirtNumber_list']:
                            temp_home_shirtNum_purple += '"'
                            temp_home_shirtNum_purple += str(shirtNum)
                            temp_home_shirtNum_purple += '"'
                            temp_home_shirtNum_purple += ','
                        home_shirtNum_purple = temp_home_shirtNum_purple[0:-1] + ')'
                        search_home_sql = 'SELECT value FROM %s WHERE shirtNumber in %s' % (home_table_name, home_shirtNum_purple)
                        cursor.execute(search_home_sql)
                        for query in cursor.fetchall():
                            value_text = query['value']
                            currency_text = value_text.split(' ')[1]
                            value_num = 0
                            if currency_text == 'Mill.':
                                try:
                                    value_num = int(value_text.split(' ')[0].replace(',',''))   # 以万欧为单位
                                except Exception as e:
                                    print('转换价格到数字出错:',e)
                                    pdb.set_trace()
                            elif currency_text == 'Th.':
                                try:
                                    value_num = int(value_text.split(' ')[0].replace(',',''))/10
                                except Exception as e:
                                    print('转换价格到数字出错:',e)
                                    pdb.set_trace()
                            home_a_value += value_num
                        home_a_value = home_a_value/11

                        # 搜索客队首发的身价】
                        away_database_name = current_season + '_league_' + chinese2english[item['home_name']]['league']
                        away_english_name = chinese2english[item['away_name']]['name']
                        away_table_name = away_database_name + '.teams_' + away_english_name
                        temp_away_shirtNum_purple = '('
                        for shirtNum in item['away_player_shirtNumber_list']:
                            temp_away_shirtNum_purple += '"'
                            temp_away_shirtNum_purple += shirtNum
                            temp_away_shirtNum_purple += '"'
                            temp_away_shirtNum_purple += ','
                        away_shirtNum_purple = temp_away_shirtNum_purple[0:-1] + ')'
                        search_away_sql = 'SELECT value FROM %s WHERE shirtNumber in %s' % (away_table_name, away_shirtNum_purple)
                        cursor.execute(search_away_sql)
                        for query in cursor.fetchall():
                            value_text = query['value']
                            currency_text = value_text.split(' ')[1]
                            value_num = 0
                            if currency_text == 'Mill.':
                                try:
                                    value_num = int(value_text.split(' ')[0].replace(',', ''))  # 以万欧为单位
                                except Exception as e:
                                    print('转换价格到数字出错:', e)
                                    pdb.set_trace()
                            elif currency_text == 'Th.':
                                try:
                                    value_num = int(value_text.split(' ')[0].replace(',', '')) / 10
                                except Exception as e:
                                    print('转换价格到数字出错:', e)
                                    pdb.set_trace()
                            away_a_value += value_num
                        away_a_value = away_a_value / 11

                        total_product_rate = home_a_value*item['home_rate'] + away_a_value*item['away_rate']
                        home_direction_probability = round(home_a_value*item['home_rate']/total_product_rate, 2)
                        away_direction_probability = round(away_a_value*item['away_rate']/total_product_rate, 2)

                        # 将第一个支持方向改为文字
                        if item['support_direction'] == 0:
                            first_direction = '0'
                        elif item['support_direction'] == 1:
                            first_direction = '主队盘口（不超过两球）'
                        elif item['support_direction'] == 0.5:
                            first_direction = '主队盘口（最多胜一球）'
                        elif item['support_direction'] == -0.5:
                            first_direction = '客队盘口（最多胜一球）'
                        elif item['support_direction'] == -1:
                            first_direction = '客队盘口（不超过两球）'

                        # 进一步分析support_direction
                        if (item['home_rate'] - item['away_rate']) >= 0.10 and home_a_value >= 2*away_a_value:
                            second_direction = '主队盘口（不超过两球）'
                        elif round(abs(item['home_rate'] - item['away_rate']),2) < 0.10 and home_a_value*2 > away_a_value:
                            second_direction = '主队不败'

                    cursor.execute('SELECT match_id FROM %s WHERE match_id=%s' % (tableName, item['match_id']))
                    table_row_len = len(cursor.fetchall())
                    print('表中存在查询数据的数目：:', table_row_len)
                    insert_sql = (
                            "INSERT INTO " + tableName + " VALUES "
                                                         "('%s', '%s', '%s', '%s', '%s', '%f','%d','%f','%d','%d','%f','%f','%s','%s')"
                    )
                    try:
                        if table_row_len < 1:
                            print('insert数据库')
                            cursor.execute(insert_sql % (
                                item['match_id'], item['match_name'], item['home_name'], item['away_name'], item['time_score'],
                                item['home_rate'], home_a_value, item['away_rate'], away_a_value, item['average_completed_match'],
                                home_direction_probability, away_direction_probability, first_direction, second_direction))
                        else:
                            if item['has_analysed']:
                                update_sql = (
                                    'UPDATE %s SET time_score="%s" WHERE match_id="%s"'
                                )
                                print('update时间或者比分信息')
                                cursor.execute(update_sql % (
                                    tableName, item['time_score'], item['match_id']))
                            else:
                                update_sql = (
                                    'UPDATE %s SET time_score="%s", home_rate=%f, home_a_value=%d, away_rate=%f, away_a_value=%d, average_completed_match=%d, home_direction_probability=%f, away_direction_probability=%f, support_direction="%s", support_direction_2="%s" WHERE match_id="%s"'
                                )
                                print('update全部信息')
                                cursor.execute(update_sql % (
                                    tableName, item['time_score'], item['home_rate'], home_a_value, item['away_rate'], away_a_value, item['average_completed_match'], home_direction_probability, away_direction_probability, first_direction, second_direction, item['match_id']))
                    except Exception as e:
                        print("数据库执行失败 ", e)
                # connection is not autocommit by default. So you must commit to save your changes.
                cursor.close()
                if not connection.commit():
                    connection.rollback()

            finally:
                connection.close()

        if spider.name == 'odds_spider':
        # 这里写爬虫 odds_spider 的逻辑
            pass
        return item
