import time
import requests as rq
import numpy as np
import pandas as pd
import random
from common_func import get_day, get_credit_day, get_con

def price_main(day=None):
    print('*** MARKET PRICE ***')
    # target day(d-1) ** CASES : all-day, done, today
    to = get_day()['biz'] if day is None else day
    fr = get_max_day()

    if fr is None:
        print('scrap for the first time')
        start = '19960701'
        get_price(start, to)
        return  # END

    start = fr.strftime("%Y%m%d")
    if to == start:
        print('already done today')
        return  # END

    # TODAY
    get_price(start, to)

def get_max_day():
    conn = get_con()
    with conn:
        cursor = conn.cursor()
        query = '''
            select max(기준일) as day 
            from   kor_market_price
            where  1=1
            and    시가총액 is not null;
            '''
        cursor.execute(query)
        return cursor.fetchone()[0]


def get_price(start, end):
    # 코스피/코스닥 OHLCV
    # http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010101
    target = {'KOSPI': '1', 'KOSDAQ': '2'}
    headers = {"Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader"}
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    update = 0

    for k in target:
        print(f'=== {k} : [{start} ~ {end}]')
        data = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT00502",
            "idxIndCd": "001",
            "indTpCd": target[k],
            "strtDd": start,
            "endDd": end,
        }
        # connection
        db = get_con()
        cursor = db.cursor()
        try:
            # request
            res = rq.post(url, data=data, headers=headers)

            if len(res.json()["output"]) <= 0:
                raise Exception(f'{res.content}')
            
            # cleansing
            df = pd.json_normalize(res.json()["output"])
            df = df.iloc[:, :6]
            df.columns = ["기준일", "종가", "시가", "고가", "저가", "거래량"]
            df.insert(1, '시장구분', k)
            df = df.replace([np.inf, -np.inf, np.nan], None)
            df["기준일"] = pd.to_datetime(df["기준일"])

            query = """
                insert into kor_market_price (기준일, 시장구분, 종가, 시가, 고가, 저가, 거래량)
                values (%s,%s,%s,%s,%s,%s,%s)
                on duplicate key update
                종가=values(종가), 시가=values(시가), 고가=values(고가), 저가=values(저가), 거래량=values(거래량);
            """
            # 결산일 데이터를 DB에 저장
            args = df.values.tolist()
            update = cursor.executemany(query, args)
            db.commit()
            time.sleep(random.randint(1,2))
            
            print(f'=== [{start} ~ {end}] {k} : {update} ')
            
        except Exception as e:
            print('error: ', e)
        finally:
            cursor.close()
            db.close()


if __name__ == '__main__':
    price_main()
    # print('day:',get_day())