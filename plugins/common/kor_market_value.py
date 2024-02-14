import numpy as np
import time
import concurrent.futures
from common.common_func import get_day,  get_connection
import random
import pandas as pd
import requests as rq

def get_target_days(isAll = False):
    engine, con = get_connection()
    try:
        # dates
        query = """
                select distinct 기준일
                from   kor_market_price
                where  1=1
                and    기준일 >= 20000101
                and    시가 is not null
            """
        if isAll:
            query += ' and 기준일 not in (select distinct 기준일 from kor_market_value)'
        else:
            query += ' and 기준일 >=  (select date_sub(now() , interval 3 DAY ) from dual)'
        target = pd.read_sql(query, con=engine)
        return target
    except Exception as e:
        print('error: ', e)
    finally:
        engine.dispose()
        con.close()


def request_all(days):

    # list = target[5000:] # Downloaded sites in 139 seconds 58000 rows
    split_list = np.array_split(days, 5)

    start_time = time.time()
    # We can use a with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # the result
        for result in executor.map(get_value, split_list):
            # clean
            if len(result) <= 0:
                continue

            df_value = pd.concat(result).reset_index(drop=True)
            df = df_value.replace(to_replace=r'[-]', value='', regex=True)
            cols = ["PER", "FPER", "PBR", "DY"]
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce', axis=1)
            df = df.replace([np.inf, -np.inf, np.nan], None)
            # DB 저장 쿼리
            query = """
                insert into kor_market_value (기준일, 시장구분, 지수명, PER, FPER, PBR, DY)
                values (%s,%s,%s,%s,%s,%s,%s)
                on duplicate key update
                PER=values(PER), FPER=values(FPER), PBR=values(PBR), DY = values(DY);
            """
            # connection
            engine, con = get_connection()
            try:
                # 결산일 데이터를 DB에 저장
                args = df.values.tolist()
                update = con.cursor().executemany(query, args)
                con.commit()
            except Exception as e:
                print('error: ', e)
            finally:
                engine.dispose()
                con.close()

            print(f"=== 업데이트 : {update} [{split_list[0]} ~ {split_list[-1]}")

    # 실행 시간 종료
    duration = time.time() - start_time
    # 결과 출력
    print(f"=== 종료 in {duration} seconds")


def get_value(target_list):
    otp_success = False
    for i in range(3):
        try:
            otp_stk = get_otp()
            otp_success = True
            break
        except Exception as e:
            print(e)
    if otp_success is False:
        raise Exception('otp fail 3 times!')

    target = {'KOSPI': '02', 'KOSDAQ': '03'}
    result = []
    
    for _day in target_list:
        # KOSPI, KOSDAQ
        for key in target:
            print(f"=== {key} : [{target_list[0]} ~ {target_list[-1]}]")
            res = None
            try:
                res = request_value(otp_stk, _day, target[key])
                time.sleep(random.randint(1, 2))
            except Exception as e:
                time.sleep(2)
            if res is not None:
                result.append(res)

        # clean
        if len(result) <= 0:
            continue

        df_value = pd.concat(result).reset_index(drop=True)
        df = df_value.replace(to_replace=r'[-]', value='', regex=True)
        cols = ["PER", "FPER", "PBR", "DY"]
        df[cols] = df[cols].apply(pd.to_numeric, errors='coerce', axis=1)
        df = df.replace([np.inf, -np.inf, np.nan], None)
        # DB 저장 쿼리
        query = """
            insert into kor_market_value (기준일, 시장구분, 지수명, PER, FPER, PBR, DY)
            values (%s,%s,%s,%s,%s,%s,%s)
            on duplicate key update
            PER=values(PER), FPER=values(FPER), PBR=values(PBR), DY = values(DY);
        """
        # connection
        engine, con = get_connection()
        try:
            # 결산일 데이터를 DB에 저장
            args = df.values.tolist()
            update = con.cursor().executemany(query, args)
            con.commit()
        except Exception as e:
            print('error: ', e)
        finally:
            engine.dispose()
            con.close()

        print(f"=== MAREKT VALUE [{target_list[0]} ~ {target_list[-1]}] 업데이트 : {update} ")
    return result


def request_value(otp, _day, code):
    headers = {
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader",
    }
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    data = {
        # "code": otp,
        "bld": "dbms/MDC/STAT/standard/MDCSTAT00701",
        "csvxls_isNo": "false",
        "idxIndMidclssCd": code,  # kospi:02 kosdaq:03
        "locale": "ko_KR",
        "searchType": "A",
        "trdDd": _day,
        "endDd": _day
    }

    # request
    res = rq.post(url, data, headers=headers)

    try:
        if res.status_code != 200:
            raise Exception("응답코드 에러!!")
        if len(res.json()["output"]) <= 0:
            return

        # cleansing
        value_df = pd.json_normalize(res.json()["output"])
        value_df = value_df.drop(columns=['CLSPRC_IDX', 'FLUC_TP_CD', 'PRV_DD_CMPR', 'FLUC_RT'])
        value_df.columns = ["지수명", "PER", "FPER", "PBR", "DY"]
        value_df.insert(0, '기준일', pd.to_datetime(_day))
        value_df.insert(1, '시장구분', 'KOSPI' if code == '02' else 'KOSDAQ')

    except Exception as e:
        print(f'error: \n{e} \n{res.content}')
        return

    return value_df


def get_otp():
    url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    data = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
        "mktId": 'STK',
        "trdDd": get_day()['biz'],
        "money": "1",
        "csvxls_isNo": "false",
        "name": "fileDown",
        "url": "dbms/MDC/STAT/standard/MDCSTAT03901",
    }
    headers = {
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader",
    }

    res = rq.post(url, data, headers=headers)
    
    if res.status_code != 200:
        raise Exception('get otp error!')
    otp_stk = res.text
    return otp_stk


def value_main(all=None):
    print('*** MARKET VALUE ***')
    days = get_target_days() if all == 'all' else get_target_days()
    days = days['기준일'].apply(lambda x: x.strftime("%Y%m%d")).to_list()

    # CASES : whole, done, today
    if len(days) == 0:
        print('already up to date')
    else:
        print('go for today')
        # request_all(days)
        get_value(days)

if __name__ == '__main__':
    # 20000101 ~ 전일
    value_main()


