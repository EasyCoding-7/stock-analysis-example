import os, environ
from pathlib import Path
import OpenDartReader
from pykrx import stock
import pandas as pd

env = environ.Env(
    # set casting, default value
    DEBUG=(bool, False)
)

class make_ana_data:
    def __init__(self, parent, using_open_dart=False):
        if using_open_dart:
            """
            ** 21-12-23 **
            * pykrx 사용하는 방향으로 선회
            * 더 필요한 데이터가 있다면 opendart에서 파싱하는 방향으로 변경예정
            """

            # using opendart
            # read env file
            base_dir = Path(__file__).resolve().parent
            environ.Env.read_env(
                env_file=os.path.join(base_dir, '.env')
            )

            api_key = env('OPENDART_API_KEY')
            dart = OpenDartReader(api_key)

            """
            small = dart.report('005930', '소액주주', 2020, reprt_code=11014)
            parent.print_tb(" just call test ", str(small))
            """

            # 2019년 모든 회사의 사업보고서
            fs_2019 = dart.finstate_all(bsns_year='2019', fs_div='CFS', reprt_code=11011)
            # 2020년 모든 회사의 3분기 보고서
            fs_2020_3Q = dart.finstate_all(bsns_year='2020', fs_div='CFS', reprt_code=11014)
        else:
            df = None
            year_list = ['20101101','20111101','20121101','20131101','20141101','20151101','20161101','20171101','20181101','20191101','20201101','20211101']
            for list in year_list:
                # get stock info
                    # stock.get_market_fundamental : BPS, PER, PBR, EPS, DIV, DPS
                df_temp1 = stock.get_market_fundamental(list)
                    # stock.get_market_cap : 시가총액, 거래량, 거래대금, 상장주식수, 외국인보유주식수
                df_temp2 = stock.get_market_cap(list)

                # merge two dataframe
                df_temp = df_temp1.join(df_temp2)

                # merge dataframes
                df_temp['year']=list[:4]
                df_temp = df_temp.reset_index()#.rename(columns={"index": "ticker"})
                if type(df) != type(None):
                    df = pd.concat([df, df_temp], ignore_index=True)
                else:
                    df = df_temp.copy()

                parent.print_tb(f" {list} ", str(df))
                # df_temp.to_excel('./test.xlsx')

        self.start()


    def start(self):
        pass