# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql.cursors
import datetime
import pdb

nowadays = datetime.datetime.now().strftime('%Y_%m_%d')
nowatime = datetime.datetime.now().strftime('%Y_%m_%d_%H%M')

class AutoTeamsRatePipeline(object):
    def process_item(self, item, spider):
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
                    "away_rate FLOAT(8) NOT NULL,"
                    "support_direction FLOAT(8) NOT NULL)"
                )
                cursor.execute(build_table % tableName)
                # 建表完成

                # 将表的安全更新模式关掉，不然不使用key column的更新成功不了
                # cursor.execute('set sql_safe_updates=off')

                cursor.execute('SELECT match_id FROM %s WHERE match_id=%s' % (tableName, item['match_id']))
                table_row_len = len(cursor.fetchall())
                print('表中存在查询数据的数目：:', table_row_len)
                insert_sql = (
                        "INSERT INTO " + tableName + " VALUES "
                                                     "('%s', '%s', '%s', '%s', '%s', '%f','%f','%f')"
                )
                update_sql = (
                        'UPDATE %s SET time_score="%s", home_rate=%f, away_rate=%f, support_direction=%f WHERE match_id="%s"'
                )
                try:
                    if table_row_len < 1:
                        print('insert数据库')
                        cursor.execute(insert_sql % (
                            item['match_id'], item['match_name'], item['home_name'], item['away_name'], item['time_score'],
                            item['home_rate'], item['away_rate'], item['support_direction']))
                    else:
                        print('update数据库')
                        cursor.execute(update_sql % (
                            tableName, item['time_score'], item['home_rate'], item['away_rate'], item['support_direction'], item['match_id']))
                except Exception as e:
                    print("数据库执行失败 ", e)
            # connection is not autocommit by default. So you must commit to save your changes.
            cursor.close()
            if not connection.commit():
                connection.rollback()

        finally:
            connection.close()
        return item
