import requests as rq
import re
import bs4
from datetime import datetime, timedelta
import pymysql
import pathlib
import configparser
from sqlalchemy import create_engine


def get_sftp():
    print('sftp 작업을 시작합니다')
    

def get_day():
    # date from naverfinance
    url = f"""https://finance.naver.com/"""
    data = rq.get(url).content

    page_data_html = bs4.BeautifulSoup(data, "html.parser")
    date_text = page_data_html.select_one(
        "#content > div.article > div.section2 > div.section_stock_market > div.group_heading > div > span#time"
    ).text.strip()

    biz_date = re.findall("[0-9]+", date_text)
    biz_date = "".join(biz_date)[:8]

    dict = {
        "biz" :biz_date,
        "biz-1" : (datetime.strptime(biz_date,("%Y%m%d")) + timedelta(days=-1)).strftime("%Y%m%d"),
        "biz-2" : get_credit_day()
    }
    return dict


def get_credit_day():
    url = "https://finance.naver.com/sise/sise_deposit.naver"
    data = rq.get(url)

    data_html = bs4.BeautifulSoup(data.content, "lxml")
    parse_day = data_html.select_one(
        "#type_0 > div > ul.subtop_chart_note > li > span.tah"
    ).text

    biz_day = re.findall("[0-9]+", parse_day)
    biz_day = "".join(biz_day)
    return biz_day



def get_con():
    # 설정파일 읽기
    config_path = pathlib.Path(__file__).parent.absolute() / "config.ini"
    config = configparser.ConfigParser()
    config.read(config_path)
    
    if 'dev' == config['env']['property']:
        con = pymysql.connect(
            user=config['db']['user'], passwd=config['db']['passwd'], host=config['db']['host'], db=config['db']['db'], charset="utf8"
        )

    return con


def get_connection():
    
    # 설정파일 읽기
    config_path = pathlib.Path(__file__).parent.absolute() / "config.ini"
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # 설장파일 색션 확인
    if 'dev' == config['env']['property']:
        con = get_con()
        engine = create_engine(config['env']['path'])
        
    return engine, con

if __name__ == '__main__':
    print(get_con())