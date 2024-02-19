from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from config.chatgpt import get_chatgpt_response
from config.pykrx_api import get_prompt_for_chatgpt
from config.telegram import send_message
import pendulum
from random import randrange
class ReportByChatgptOperator(BaseOperator):
    def __init__(self, post_cnt_per_market: int, model='gpt-3.5-turbo', **kwargs):
        super().__init__(**kwargs)
        self.post_cnt_per_market = post_cnt_per_market
        self.model = model
    

    def execute(self, context):
        chatgpt_api_key = Variable.get('chatgpt_api_key')
        telegram_token = Variable.get('telegram_token')
        chat_id = "-1002038105003"

        now =  pendulum.now('Asia/Seoul')
        now_yyyymmmdd = now.strftime('%Y%m%d')
        yyyy = now.year
        mm = now.month
        dd = now.day
        hh = now.hour
        mi = now.minute
        kospi_ticker_name_lst, kospi_fluctuation_rate_lst, prompt_of_kospi_top_n_lst = get_prompt_for_chatgpt(now_yyyymmmdd, market='KOSPI', cnt=self.post_cnt_per_market)
        kosdaq_ticker_name_lst, kosdaq_fluctuation_rate_lst, prompt_of_kosdaq_top_n_lst = get_prompt_for_chatgpt(now_yyyymmmdd, market='KOSDAQ', cnt=self.post_cnt_per_market)
        
        tot_ticker_name_lst = kospi_ticker_name_lst + kosdaq_ticker_name_lst
        tot_fluctuation_rate_lst = kospi_fluctuation_rate_lst + kosdaq_fluctuation_rate_lst
        tot_prompt = prompt_of_kospi_top_n_lst + prompt_of_kosdaq_top_n_lst

        market = 'KOSPI'
        for idx, prompt in enumerate(tot_prompt):
            temperature = randrange(10,100)/100     # 0.1 ~ 1 사이 
            ticker_name = tot_ticker_name_lst[idx]
            print(f'ticker: {ticker_name}, temperature:{temperature}')      # temperature 확인용 로깅

            fluctuation_rate = tot_fluctuation_rate_lst[idx]
            fluctuation_rate = round(fluctuation_rate, 1)
            chatgpt_resp = get_chatgpt_response(api_key=chatgpt_api_key, 
                                                prompt=prompt,
                                                temperature=temperature)
            if chatgpt_resp == '': 
                print(f'{ticker_name} pass')
                continue
            
            # chatgpt_resp = chatgpt_resp.replace('\n','<br/>')
            
            if idx >= self.post_cnt_per_market:
                market = 'KOSDAQ'
                
            title = f'{yyyy}/{mm}/{dd} {hh}시 {mi}분 {market} 급등 {fluctuation_rate}% {ticker_name} 주목!'
            print(f'title:{title}')
            print(f'content:{chatgpt_resp}')
            
            message = f'{title}\n\n {chatgpt_resp}'
            send_message(telegram_token, message, chat_id)
            # set_tistory_post(access_token=tistory_access_token,
            #                  blog_name='hjkim-sun',
            #                  title=f'{yyyy}/{mm}/{dd} {hh}시 {market} 급등 {fluctuation_rate}% {ticker_name} 주목!',
            #                  content=chatgpt_resp,
            #                  tag_lst=[f'{market}급등','급등주',ticker_name])
            
