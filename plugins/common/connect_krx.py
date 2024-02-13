# from common.common_func import *
# from common.common_func import get_day
from common.common_func import get_day, get_con
import requests as rq


def try_con():
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
    print(res.text)

if __name__ == '__main__':
    print('main')
    print(get_con())
    # print(try_con())