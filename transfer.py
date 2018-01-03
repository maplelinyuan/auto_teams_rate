from lxml import etree
import pdb
import pymysql.cursors

# 要修改的参数
league_db_name = '17t18_league_10'   # 联赛名称 具体见readme
team_name = 'Virtus_Entella'    # 队伍名称

with open("record.txt", "rb") as f:
    text = f.read().decode('utf-8')
page = etree.HTML(text)

player_list = []
for tr in page.xpath( '//tbody/tr'):
    player_dict = {}
    player_dict['shirtNumber'] = tr.xpath('td')[0].xpath('div/text()')[0]
    # 如果没有编号就跳过
    if not player_dict['shirtNumber'].isdigit():
        continue
    player_dict['name'] = tr.xpath('td')[1].xpath('table/tr')[0].xpath('td')[1].xpath('div')[0].xpath('span/a/text()')[0]
    player_dict['position'] = tr.xpath('td')[1].xpath('table/tr')[1].xpath('td/text()')[0]
    player_dict['value'] = tr.xpath('td')[-1].xpath('text()')[0]
    player_list.append(player_dict)

# Connect to the database
db_name = league_db_name
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
        tableName = 'teams_' + team_name  # 表名
        # 建立当前队伍表
        build_table = (
            "CREATE TABLE IF NOT EXISTS "' %s '""
            "(shirtNumber VARCHAR(5) NOT NULL PRIMARY KEY,"
            "name VARCHAR(50) NOT NULL,"
            "position VARCHAR(50) NOT NULL,"
            "value VARCHAR(50) NOT NULL)"
        )
        cursor.execute(build_table % tableName)
        # 建表完成

        for player in player_list:
            search_sql = 'SELECT shirtNumber FROM %s WHERE shirtNumber=%s' % (tableName, player['shirtNumber'])
            cursor.execute(search_sql)
            table_row_len = len(cursor.fetchall())
            # print('表中存在查询数据的数目：:', table_row_len)
            insert_sql = (
                    "INSERT INTO " + tableName + " VALUES "
                                                 "('%s', '%s', '%s', '%s')"
            )
            try:
                if table_row_len < 1:
                    print('insert数据库')
                    cursor.execute(insert_sql % (
                        player['shirtNumber'], player['name'].replace("'",' '), player['position'], player['value']))
                else:
                    update_sql = (
                        'UPDATE %s SET name="%s", position="%s", value="%s" WHERE shirtNumber="%s"'
                    )
                    print('update数据库')
                    cursor.execute(update_sql % (
                        tableName, player['name'].replace("'",' '), player['position'], player['value'], player['shirtNumber']))
            except Exception as e:
                print("数据库执行失败 ", e)
    # connection is not autocommit by default. So you must commit to save your changes.
    cursor.close()
    if not connection.commit():
        connection.rollback()

finally:
    connection.close()