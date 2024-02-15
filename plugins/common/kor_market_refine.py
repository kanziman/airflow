import numpy as np
from common.common_func import get_connection
import pandas as pd

def update_adr(isAll=False):
    # db
    engine, con = get_connection()
    try:
        query = '''
        select
               sum(case when 시장구분='KOSPI' and 종가 > 시가 then 1 else 0 end) as p_up,
               sum(case when 시장구분='KOSPI' and 시가 > 종가 then 1 else 0 end) as p_dn,
               sum(case when 시장구분='KOSDAQ' and 종가 > 시가 then 1 else 0 end) as d_up,
                sum(case when 시장구분='KOSDAQ' and 시가 > 종가 then 1 else 0 end) as d_dn,
                기준일
            from (select *
                    from kor_price price
                    join (select 종목코드 as code, 시장구분
                            from kor_ticker
                            where 기준일 = (select max(기준일) from kor_ticker)
                          ) ticker
                    on    price.종목코드 = ticker.code
                    {0}
                    ) t
            group by 기준일;
        '''
        if isAll:
            query = query.format(' where price.기준일 >= "2010-01-01"')
        else:
            query = query.format(' where price.기준일 >=  (select date_sub(now() , interval 1 MONTH ) from dual)')
        print(query)
        adrs = pd.read_sql(query, con=engine)
    except Exception as e:
        raise Exception('select error!!', e)
    finally:
        con.close()

    df = adrs[["p_up", "p_dn", "d_up", "d_dn"]].rolling(window=20, min_periods=20).sum()
    df["기준일"] = adrs["기준일"]
    df = df.dropna(axis=0).reset_index(drop=True)
    df["KOSPI"] = (df["p_up"] / df["p_dn"]).round(4) * 100
    df["KOSDAQ"] = (df["d_up"] / df["d_dn"]).round(4) * 100
    df = df.iloc[:, 4:]
    df = df.melt(id_vars=["기준일"])
    df["지수명"] = np.where(df["variable"] == "KOSPI", "코스피", "코스닥")
    df = df.rename(columns={"variable": "시장구분", "value": "adr"})

    query = """
        insert into kor_market_value (기준일, 시장구분, adr, 지수명)
        values (%s,%s,%s,%s)
        on duplicate key update
        adr=values(adr);
    """
    # connection
    engine, con = get_connection()
    try:
        # 결산일 데이터를 DB에 저장
        args = df.values.tolist()
        update = con.cursor().executemany(query, args)
        con.commit()
        print(f'adr update : {update}')
    except Exception as e:
        print('error: ', e)
    finally:
        engine.dispose()
        con.close()


def market(isAll=False):
    print("MARKET REFINE")
    engine, con = get_connection()
    try:
        select_query = f"""
            select a.기준일, a.시장구분, 시가, 고가, 저가, 종가,
                   거래량, 거래대금, 시가총액, 외국인시가총액,
                   b.PER, b.PBR, b.DY, 신용잔고, 예탁금, b.adr
            from kor_market_price a
            join (select * from kor_market_value where 지수명 in ('코스피','코스닥')) b on a.기준일 = b.기준일 and a.시장구분 = b.시장구분;
        """
        if isAll:
            select_query = select_query.format(' where a.기준일 >= "2010-01-01"')
        else:
            select_query = select_query.format(' where a.기준일 >=  (select date_sub(now() , interval 1 MONTH ) from dual)')
        print(select_query)
        
        target = pd.read_sql(select_query, con=engine)
        target = target.replace([np.inf, -np.inf, np.nan], None)
        
        # INSERT
        insert_query = f"""
            insert into market (baseDate,mktType,open,high,low
                                ,close,volume,amount,value,fValue
                                ,per,pbr,dy,credit,deposit,adr)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            on duplicate key update
            open=values(open),high=values(high),low=values(low),close=values(close),
            volume=values(volume),amount=values(amount),value=values(value),fValue=values(fValue),
            per=values(per),pbr=values(pbr),dy=values(dy),credit=values(credit),deposit=values(deposit),adr=values(adr);
        """
        cursor = con.cursor()
        args = target.values.tolist()
        update = cursor.executemany(insert_query, args)
        con.commit()
        print(f'up : {update}')
    except Exception as e:
        print('error: ', e)
    finally:
        engine.dispose()
        con.close()


def refine():
    print('update adr')
    update_adr()
    print('select insert market')
    market()


if __name__ == '__main__':
    refine()


