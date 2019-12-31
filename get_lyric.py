import json

import os
import pymysql
import re
import requests
import time
from scrapy.selector import Selector

import config

header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}


class Cloud163Lyric:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
            'Referer': 'http://music.163.com/'}
        self.main_url = 'http://music.163.com/'
        self.session = requests.Session()
        self.session.headers = self.headers
        self.rstr = r"[\/\\\:\*\?\"\<\>\|]"

    def get_songurls(self,playlist):
        """进入所选歌单页面，得出歌单里每首歌各自的ID 形式就是“song?id=64006\""""
        url = self.main_url+'playlist?id=%d'% playlist
        re = self.session.get(url)      # 直接用session进入网页，懒得构造了
        sel = Selector(text=re.text)    # 用scrapy的Selector，懒得用BS4了
        song_urls = sel.xpath('//ul[@class="f-hide"]/li/a/@href').extract()
        return song_urls                # 所有歌曲组成的list
        # ['/song?id=64006', '/song?id=63959', '/song?id=25642714', '/song?id=63914', '/song?id=4878122', '/song?id=63650']

    def get_songinfo(self,songurl):
        '''根据songid进入每首歌信息的网址，得到歌曲的信息
        return：'64006'，'陈小春-失恋王'''
        url = self.main_url+songurl
        re = self.session.get(url)
        sel = Selector(text=re.text)
        song_id = url.split('=')[1]
        song_name = sel.xpath("//em[@class='f-ff2']/text()").extract_first()
        singer = '&'.join(sel.xpath("//p[@class='des s-fc4']/span/a/text()").extract())
        song_name = singer+'-'+song_name
        return str(song_id), song_name

    def download_song(self, song_url, dir_path):
        """根据歌曲url，下载mp3文件"""
        song_id, song_name = self.get_songinfo(song_url)  # 根据歌曲url得出ID、歌名
        song_url = 'http://music.163.com/song/media/outer/url?id=%s.mp3'%song_id
        path = dir_path + os.sep + song_name + '.mp3'  # 文件路径
        requests.urlretrieve(song_url, path)  # 下载文件

    def work(self, playlist):
        song_urls = self.get_songurls(playlist)  # 输入歌单编号，得到歌单所有歌曲的url
        dir_path = r'C:\Users\Administrator\Desktop'
        for song_url in song_urls:
            self.download_song(song_url, dir_path)  # 下载歌曲

    def get_music(self, music_path, song_name, song_id):
        song_url = 'http://music.163.com/song/media/outer/url?id=%s.mp3'% song_id
        path = music_path + os.sep + song_name + '.mp3'
        response = requests.get(song_url, headers=header)

        with open(path, mode='wb') as file:
            file.write(response.content)
        print('歌曲 ' + song_name + '下載完成')

    def get_lyric(self, folder_name, song_id, song_name, song_singer):
        lyric_url = 'http://music.163.com/api/song/lyric?' + 'id=' + str(song_id) + '&lv=1&kv=1&tv=-1'
        html = requests.post(lyric_url, headers=header)
        json_obj = html.text

        path = './Music/{}_lrc'.format(folder_name)
        if not os.path.exists(path):
            os.mkdir(path)
        file_path = os.path.join(path, re.sub(self.rstr, "", song_name) + '_' + re.sub(self.rstr, "", song_singer) + '.txt')

        try:
            j = json.loads(json_obj)
            lyric = j['lrc']['lyric']       # source_lyric
            regex = re.compile(r'\[.*\]')   # 利用正则表达式去除时间轴
            lyric = re.sub(regex, '', lyric)
            lyric = lyric.strip()

            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(lyric)
                    print("\\\\\\ the searched lyric of id<%s> [%s_%s] 下载完成 " % (song_id, song_name, song_singer))
                    return 1
            except Exception as e:
                print("{}-{}下载出错 id:{} , {}".format(song_name, song_singer, song_id, e))
                return 3

            txtfile.close()
        except Exception as e:
            print('{}_{} json load error:{}'.format(song_name, song_singer, e))
            return 2


def load_info(folder_name):
    once_load_limit = 5  # 单次获取 数据库信息 数量,尽量小
    cycle = 30      # 循环次数
    count = 0
    for i in range(0, cycle):
        print('--' * 15, ' {0}th load from database <<{1}>>'.format(i + 1, folder_name), '--' * 15)

        sql = 'select distinct song_id,name,singer from %s where get_lyric = 0 and get_music <= 2 LIMIT %s,%s' % (
            folder_name, str(0), str(once_load_limit))
        try:
            cursor.execute(sql)
            results = cursor.fetchall()

            for row in results:
                count += 1
                song_id = row[0]
                song_name = row[1]
                song_singer = row[2]
                print('processing {}th song: '.format(count), '[' + song_name + '_' + song_singer + ']')

                if not song_id:
                    signal = 10
                    print('\\\\\\ [{}_{}] has no song_id, mission skip to next'.format(song_name, song_singer))
                else:
                    signal = downloader.get_lyric(folder_name, song_id, song_name, song_singer)

                try:
                    sql_update_music = 'UPDATE {0} SET get_lyric = {1}\
                        WHERE name = "{2}" and singer = "{3}"'\
                        .format(folder_name, signal, song_name, song_singer)
                    cursor.execute(sql_update_music)
                    db.commit()
                except Exception as e:
                    db.rollback()
                    print('{}_{}update get_lyric  failed :'.format(song_name, song_singer), e)

                time.sleep(10)
        except Exception as e:
            print("ERROR : Unable to fetch data {}_{}  id:{}\t{}".format(song_name, song_singer, song_id, e))


if __name__ == '__main__':
    db = pymysql.connect(config.db_configs['host'], config.db_configs['user'], config.db_configs['password'], config.db_configs['db'])
    print('db connection is :', db.ping())
    cursor = db.cursor()
    downloader = Cloud163Lyric()

    list_start = 1
    list_end = 8
    for list_index in range(list_start-1, list_end):
        folder_name = config.LIST[list_index].replace('-', '_')
        load_info(folder_name)
    db.close()
    print('db disconnect ...finished')
