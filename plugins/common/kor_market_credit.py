import numpy as np
import requests as rq
import pandas as pd
from common_func import get_day, get_con

def get_max_day():
    conn = get_con()
    with conn:
        cursor = conn.cursor()
        query = '''
            select max(기준일) as day 
            from   kor_market_price
            where  1=1
            and    시가총액 is not null
            and    신용잔고 is not null;
            '''
        cursor.execute(query)
        return cursor.fetchone()[0]


def get_credit_deposit(start, end):
    # merge
    if start > end:
        print(f"날짜 에러 {start} {end}")
        return
    try:
        credit = get_credit(end, start)
        deposit = get_market_deposit(start, end)
        merge_df = pd.merge(
            credit, deposit, on=["기준일"], how="left"
        )
        merge_df = merge_df.replace([np.inf, -np.inf, np.nan], None)
    except Exception as e:
        raise Exception('request error!!', e)

    # DB
    con = get_con()
    try:
        query = """
            insert into kor_market_price (기준일, 시장구분, 신용잔고, 예탁금)
            values (%s,%s,%s,%s)
            on duplicate key update
            신용잔고=values(신용잔고) , 예탁금=values(예탁금);
        """
        # 결산일 데이터를 DB에 저장
        args = merge_df.values.tolist()
        update = con.cursor().executemany(query, args)
        con.commit()
        print(f'=== CREDIT [{start} ~ {end}]  : {update}')
    except Exception as e:
        raise Exception('DB error!!', e)
    finally:
        con.close()


def get_credit(end, start):
    url = "https://freesis.kofia.or.kr/meta/getMetaDataList.do"
    headers = {"Referer": "https://freesis.kofia.or.kr/stat/FreeSIS.do"}
    data = {
        "dmSearch": {
            "tmpV40": "1000000",  # 백만
            "tmpV41": "1",  # 천주
            "tmpV1": "D",
            "tmpV45": start,
            "tmpV46": end,
            "OBJ_NM": "STATSCU0100000070BO"
        }
    }
    # request
    res = rq.post(url, json=data, headers=headers)

    try:
        df = pd.json_normalize(res.json().get('ds1'))
        df = df.iloc[:, :4]
        cols = ['기준일', '신용잔고', 'KOSPI', 'KOSDAQ']
        df.columns = cols
        df = df.drop(columns=['신용잔고'])
        df = df.melt(id_vars=["기준일"])
        cols = ['기준일', '시장구분', '신용잔고']
        df.columns = cols
    except Exception as e:
        raise Exception('credit error!!')

    return df


def get_market_deposit(start, end):
    url = "https://freesis.kofia.or.kr/meta/getMetaDataList.do"
    headers = {"Referer": "https://freesis.kofia.or.kr/stat/FreeSIS.do"}
    data = {
        "dmSearch": {
            "tmpV40": "1000000",  # 백만
            "tmpV41": "1",  # 천주
            "tmpV1": "D",
            "tmpV45": start,
            "tmpV46": end,
            "OBJ_NM": "STATSCU0100000060BO"
        }
    }
    # request
    res = rq.post(url, json=data, headers=headers)
    try:
        deposit = pd.json_normalize(res.json().get('ds1'))
        deposit = deposit.iloc[:, :2]
        cols = ['기준일', '예탁금']
        deposit.columns = cols
    except Exception as e:
        raise Exception('deposit error!!')
    return deposit


def credit_main(day=None):
    print('*** MARKET CREDIT ***')
    # target day(d-1)
    to = get_day()['biz-1'] if day is None else day
    print(f'=== biz_day : {to}')

    # CASES : all-day, done, today
    fr = get_max_day()
    if fr is None:
        print('=== scrap for the first time')
        start = '20000104'
        get_credit_deposit(start, to)
        return

    start = fr.strftime("%Y%m%d")
    if to <= start:
        print('=== already up to date')
        return

    # TODAY
    print('=== scrap for today')
    get_credit_deposit(start, to)


if __name__ == '__main__':
    credit_main()
