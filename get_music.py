import config
import pymysql
from CloudMusic import Music163Menu
import time


def load_info(folder_name):
    once_load_limit = 5     # 单次获取 数据库信息 数量,尽量小
    cycle = 25              # 循环次数
    count = 0
    for i in range(0, cycle):
        print('--'*15, ' {0}th load from database <<{1}>>'.format(i+1, folder_name), '--'*15)

        sql = 'select distinct name,singer from %s where get_music = 0 LIMIT %s,%s' % (folder_name, str(0), str(once_load_limit))
        sql = 'select distinct name,singer,get_music from %s where get_music2 = 0 LIMIT %s,%s' % \
              (folder_name, str(0), str(once_load_limit))

        try:
            cursor.execute(sql)
            results = cursor.fetchall()

            for row in results:
                count += 1
                name = row[0]
                singer = row[1]
                print('processing {}th song: '.format(count), '[' + name + '_' + singer + ']')
                signal, song_id = Music163Menu.scheduled_downloader(name, singer, folder_name, False)
                print('##### signal of this process is : {}'.format(signal))

                try:
                    if song_id:
                        sql_update_music = 'UPDATE {0} SET get_music2 = {1},song_id = {2} \
                            WHERE name = "{3}" and singer = "{4}"'\
                            .format(folder_name, signal, song_id, name, singer)
                    else:
                        sql_update_music = 'UPDATE {0} SET get_music2 = {1}\
                                                    WHERE name = "{2}" and singer = "{3}"' \
                            .format(folder_name, signal, name, singer)
                    cursor.execute(sql_update_music)
                    db.commit()
                except Exception as e:
                    db.rollback()
                    print('{}_{}update get_music or song_id failed :'.format(name, singer), e)

                if row[2] != signal:
                    time.sleep(10)
                    signal = Music163Menu.scheduled_downloader(name, singer, folder_name, True)
                    print('##### different song_id lead download signal of this process is : {}'.format(signal))
                print()
                time.sleep(10)
        except Exception as e:
            print("ERROR : Unable to fetch data {}_{} \t{}".format(name, singer, e))
            break


if __name__ == '__main__':
    db = pymysql.connect(config.db_configs['host'], config.db_configs['user'], config.db_configs['password'], config.db_configs['db'])
    print('db connect success')
    cursor = db.cursor()

    list_start = 3  # config.LIST[位置]
    list_end = 8
    for list_index in range(list_start-1, list_end):
        folder_name = config.LIST[list_index].replace('-', '_')
        load_info(folder_name)

    db.close()


