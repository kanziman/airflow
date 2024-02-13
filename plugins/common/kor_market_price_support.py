import numpy as np
import requests as rq
import pandas as pd
from common_func import get_day, get_con

def get_price_kofia(start, end):
    print('*** MARKET PRICE SUPPORT ***')
    url = "https://freesis.kofia.or.kr/meta/getMetaDataList.do"
    headers = {"Referer": "https://freesis.kofia.or.kr/stat/FreeSIS.do"}
    target = {'KOSPI': 'STATSCU0100000020BO', 'KOSDAQ': 'STATSCU0100000030BO'}

    for k in target:
        print(f'=== kofia {k} : [{start} ~ {end}]')

        data = {
            "dmSearch": {
                "tmpV40": "1000000",
                "tmpV41": "1000",
                "tmpV1": "D",
                "tmpV45": start,
                "tmpV46": end,
                "OBJ_NM": target[k]
            }
        }

        try:
            # request
            res = rq.post(url, json=data, headers=headers)
            # print(f'status code: {res.status_code}')

            # res.text
            df = pd.json_normalize(res.json().get('ds1'))
            df = df.drop(columns=['TMPV3', 'TMPV7'])  # volume
            df.columns = ['기준일', '종가', '거래대금', '시가총액', '외국인시가총액']
            df.insert(1, '시장구분', k)
            df = df.replace([np.inf, -np.inf, np.nan], None)

        except Exception as e:
            raise Exception('request error!!', e)


        # connection
        con = get_con()
        cursor = con.cursor()
        try:
            # DB 저장 쿼리
            query = """
                insert into kor_market_price (기준일, 시장구분, 종가, 거래대금, 시가총액, 외국인시가총액)
                values (%s,%s,%s,%s,%s,%s)
                on duplicate key update
                거래대금=values(거래대금), 시가총액=values(시가총액), 외국인시가총액=values(외국인시가총액);
            """

            # 결산일 데이터를 DB에 저장
            args = df.values.tolist()
            update = cursor.executemany(query, args)
            con.commit()
            
            print(f'=== [{start} ~ {end}] {k} : {update} ')

        except Exception as e:
            raise Exception('DB error!!', e)
        finally:
            con.close()


def get_max_day():
    db = get_con()
    cursor = db.cursor()
    try:
        query = '''
        select max(기준일) as day 
        from   kor_market_price
        where  1=1
        and    시가총액 is not null;
        '''
        cursor.execute(query)
        return cursor.fetchone()[0]

    except Exception as e:
        print('error: ', e)
    finally:
        cursor.close()
        db.close()


def price_support_main(day=None):

    # target day(d-1)
    to = get_day()['biz-1'] if day is None else day
    print(f'=== biz_day : {to}')

    try:
        # CASES : all-day, done, today
        fr = get_max_day()
        if fr is None:
            print('=== scrap for the first time')
            start = '19960701'
            get_price_kofia(start, to)
            return  # END

        start = fr.strftime("%Y%m%d")
        if to == start:
            print('=== already done today')
            get_price_kofia(start, to)
            return  # END

        # TODAY
        print('=== scrap for today')
        get_price_kofia(start, to)

    except Exception as e:
        print(e)

if __name__ == '__main__':
    # 시가총액 / 외국인 비중 (하루 전일자 기준)
    price_support_main()