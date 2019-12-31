# input your db configuration here
db_configs = {'host': 'localhost', 'user': 'root', 'password': 'shaven', 'db': 'cloud_music'}

# download target tables from your db
LIST = [
    'pop-songs',
    'country-songs',
    'rock-songs',
    'r-b-hip-hop-songs',
    'latin-songs',
    'dance-electronic-songs',
    'christian-songs',
    'hot-holiday-songs',
]

'''
get_music 回执释义：
    1  精确匹配 下载成功
    2  模糊匹配 下载成功
    3  精确/模糊匹配 下载失败
    4  有搜索返回，但无匹配结果
    5  有精确/模糊匹配  无版权无法下载
    10 无搜索结果

'''
'''
get_lrc 回执释义 
    1   成功下载id歌词
    2   未找到id歌词
    3   下载出错
'''