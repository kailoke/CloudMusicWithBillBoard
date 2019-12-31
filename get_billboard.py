import time
import billboard
from Model.db_helper import DbHelper
from config import LIST
from config import db_configs


# modify table_name(in SQL) from LIST,cause SQL DO NOT support "-" in table name
def table_format(index):
    return LIST[index].replace('-', '_')


def get_chart_info(index, start_date, end_date):
    table_name = table_format(index)
    print("Start dealing with Table: ", table_name)

    # duplicate check
    db_index = 1
    # success and failed registers:
    counter = 1
    null_data = 0
    null_data_list = []

    # Crawling
    while int(start_date.replace("-", "")) > int(end_date.replace("-", "")):
        if start_date:
            chart = billboard.ChartData(LIST[index], date=start_date)
            print('previousData:', chart.previousDate, "the ", counter + "th process")
            counter += 1
            for item in chart:
                db.save_chart_item(start_date, item.title, item.artist, item.peakPos, db_index, table_name)
                db_index += 1
            start_date = chart.previousDate

        else:
            null_data += 1
            null_data_list.append(start_date)
        print("第{}次爬取完成，程序休眠20秒".format(counter))
        time.sleep(20)


if __name__ == '__main__':
    db = DbHelper()
    db.connect(db_configs)

    get_chart_info(0)

    db.close()
