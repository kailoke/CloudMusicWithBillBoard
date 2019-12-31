import time
import pymysql

'''
数据库操作类
所需表：
    'pop-songs',
    'country-songs',
    'rock-songs',
    'r-b-hip-hop-songs',
    'latin-songs',
    'dance-electronic-songs',
    'christian-songs',
    'hot-holiday-songs',
建表语句：
create table if not exists TABLE-NAME(id int primary key auto_increment,author varchar(30),video_id varchar(25),description text,like_count int(9),comment_count int(7),share_count int(8),music_author varchar(30),music_title varchar(50),filename text,download_url text,create_time datetime);
'''


class DbHelper(object):
    def __init__(self):
        self.mutex = 0  # 锁信号
        self.db = None

    def connect(self, configs):
        try:
            self.db = pymysql.connect(
                host=configs['host'],
                user=configs['user'],
                password=configs['password'],
                db=configs['db'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            print('db connect success')
            return self.db
        except Exception as e:
            print('db connect fail,error:', str(e))
            return None

    def close(self):
        if self.db:
            self.db.close()
            print('db close')

    def save_one_data_to_video(self, data):
        while self.mutex == 1:  # connection正在被其他线程使用，需要等待
            time.sleep(1)
            print('db connect is using...')
        self.mutex = 1  # 锁定
        try:
            with self.db.cursor() as cursor:
                sql = 'insert into video(author,video_id,description,like_count,comment_count,share_count,music_author,' \
                      'music_title,filename,download_url,create_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())'
                cursor.execute(sql, (
                    data['author'], data['video_id'], data['description'], data['like_count'], data['comment_count'],
                    data['share_count'], data['music_author'], data['music_title'], data['filename'],
                    data['download_url']))
                self.db.commit()
                # self.mutex = 0  # 解锁
                print('{}\t{} insert into video'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                                                        data['video_id']))
        except Exception as e:
            print('{}\tsave video_id:{} fail,error:{}'.format(
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), data['video_id'], str(e)))
        finally:
            self.mutex = 0  # 解锁

    def save_one_data_to_comment(self, data):
        while self.mutex == 1:  # connection正在被其他线程使用，需要等待
            time.sleep(1)
            print('db connect is using...')
        self.mutex = 1  # 锁定
        try:
            with self.db.cursor() as cursor:
                sql = 'insert into comment(video_id,user,content,like_count,comment_time,beReplied_user,' \
                      'beReplied_content,beReplied_like_count,beReplied_comment_time,create_time) ' \
                      'values(%s,%s,%s,%s,%s,%s,%s,%s,%s,now())'
                cursor.execute(sql, (
                    data['video_id'], data['user'], data['content'], data['like_count'], data['comment_time'],
                    data['beReplied_user'], data['beReplied_content'], data['beReplied_like_count'],
                    data['beReplied_comment_time']))
                self.db.commit()
                self.mutex = 0  # 解锁
                print('{}\tuser:{} comment insert into comment'.format(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), data['user']))
        except Exception as e:
            print('{}\tsave user:{} comment fail,error:{}'.format(
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), data['user'], str(e)))
        finally:
            self.mutex = 0  # 解锁

    def save_chart_item(self, date, name, singer, peak, count, chart_name):
        while self.mutex == 1:  # connection正在被其他线程使用，需要等待
            print('db connect is using...,wating 1 sec to retry')
            time.sleep(1)

        self.mutex = 1  # 锁定
        sql = 'INSERT INTO {}(date, name, singer, peak, get_music, get_lyric) \
                values (%s, %s, %s, %s, %s, %s)'.format(chart_name)
        sql_fetch = 'select * from {} where id = %s'.format(chart_name) % count
        with self.db.cursor() as cursor:
            try:
                cursor.execute(sql_fetch)
                results = cursor.fetchone()
            except Exception as e:
                print("save_music_info fetch db error:", e)
            if results:
                print("{}th\t{} has already existed..".format(count, name))
                self.mutex = 0  # 解锁
                return
            try:
                cursor.execute(sql, (date, name, singer, peak, 0, 0))
                self.db.commit()
                self.mutex = 0  # 解锁
                print('{}\t{}th\tsong:{} info success inserted into {}'. \
                      format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), count, name, chart_name))
            except Exception as e:
                self.db.rollback()
                print('{}\t{}th\tsong:"{}" info save failed ,error:{}'.format(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), name, str(e)))
            finally:
                self.mutex = 0  # 解锁

    def find_today_video(self):
        try:
            with self.db.cursor() as cursor:
                sql = 'select video_id from video where DATE(create_time) = CURDATE()'
                cursor.execute(sql)
                res = cursor.fetchall()
                return res
        except Exception as e:
            print('find_today_video fail,error:', str(e))
            return None

    # def find_all_detail(self):
    #     try:
    #         with self.db.cursor() as cursor:
    #             sql = 'select url,filename from detail limit 10'
    #             cursor.execute(sql)
    #             res = cursor.fetchall()
    #             return res
    #     except Exception as e:
    #         print('find_all_detail fail,error:', str(e))
    #         return None
