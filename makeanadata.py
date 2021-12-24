import os, environ
from pathlib import Path
import OpenDartReader
from pykrx import stock
import pandas as pd
import numpy as np

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
            buy_start_date = '0101'  # MMDD
            but_end_date = '1230'
            trading_ticker_number=15
            total_ror_list = []
            total_mdd_list = []
            year_list = ['20101101','20111101','20121101','20131101','20141101','20151101','20161101','20171101','20181101','20191101','20201101','20211101']

            for list in year_list:
                # get stock info
                    # stock.get_market_fundamental : BPS, PER, PBR, EPS, DIV, DPS
                df_temp1_ = stock.get_market_fundamental(list, market="KOSDAQ")
                df_temp1__ = stock.get_market_fundamental(list, market="KOSPI")
                df_temp1 = pd.concat([df_temp1_, df_temp1__])
                    # stock.get_market_cap : 시가총액, 거래량, 거래대금, 상장주식수, 외국인보유주식수
                df_temp2_ = stock.get_market_cap(list, market="KOSDAQ")
                df_temp2__ = stock.get_market_cap(list, market="KOSPI")
                df_temp2 = pd.concat([df_temp2_, df_temp2__])

                # merge two dataframe
                df_temp = df_temp1.join(df_temp2)
                df_temp['year']=list[:4]

                """
                Filter
                """
                # 시가총액 하위 20% 계산
                market_cap_quantile_series = df_temp.groupby("year")['시가총액'].quantile(.2)
                filtered_df = df_temp.join(market_cap_quantile_series, on="year", how="left", rsuffix="20%_quantile")
                filtered_df = filtered_df[filtered_df['시가총액'] <= filtered_df['시가총액20%_quantile']]
                # parent.print_tb(" 시가 총액 하위 20%만 남기고 필터링", str(filtered_df))

                """
                Selector
                """
                # Selector - 1 : PBR >= 0.2
                filtered_df = filtered_df[filtered_df['PBR'] >= 0.2]
                # parent.print_tb(" PBR 0.2 이상 필터링", str(filtered_df))

                # Selector - 2 : 15개 종목 필터링
                smallest_pbr_series = filtered_df.groupby("year")['PBR'].nsmallest(trading_ticker_number)
                # parent.print_tb(f" 년도별 하위 {trading_ticker_number}개 필터링", str(smallest_pbr_series))

                total_ror = 0
                total_mdd = 0

                # 상장폐지일시 df가 empty
                delisting_cnt = 0
                for ticker in smallest_pbr_series.index:
                    # print(ticker[1])
                    df = stock.get_market_ohlcv(str(list[:4]+buy_start_date), str(list[:4]+but_end_date), ticker[1])
                    ror, mdd, df = self.calculate_ror_mdd(df)
                    if df.empty:
                        delisting_cnt += 1
                    else:
                        # parent.print_tb(" df", str(df))
                        # parent.print_tb(f" {ticker[1]} ", "ror : " + str(ror) + " | mdd : " + str(mdd))
                        total_ror += ror
                        total_mdd += mdd
                total_ror = total_ror / (trading_ticker_number - delisting_cnt)
                total_mdd = total_mdd / (trading_ticker_number - delisting_cnt)
                total_ror_list.append(total_ror)
                total_mdd_list.append(total_mdd)
                parent.print_tb(f" {list[:4]} ", "ror : " + str(total_ror) + " | mdd : " + str(total_mdd))

            total_ror = np.prod(total_ror_list)
            total_mdd = np.mean(total_mdd_list)

            parent.print_tb(f" total ", "ror : " + str(total_ror) + " | mdd : " + str(total_mdd))

            """
            # 참고용 이후 지울 것
                # merge dataframes
                df_temp = df_temp.reset_index()#.rename(columns={"index": "ticker"})
                if type(df) != type(None):
                    df = pd.concat([df, df_temp], ignore_index=True)
                else:
                    df = df_temp.copy()

                parent.print_tb(f" {list} ", str(df))
                # df_temp.to_excel('./test.xlsx')

            #df_ = df.pivot(index="year", column="티커",)
            """

        self.start()

    def calculate_ror_mdd(self, df):
        # calculate ror
        df_temp = df
        ror = 0
        for price in df_temp.iterrows():
            if price[1]['시가'] != 0:
                ror = df.iloc[len(df)-1]['종가'] / price[1]['고가']
                break
            else:
                # 구매당시 거래정지는 제외함.
                df = df.drop([price[0]])

        # calculate mdd
        mdd = (df['고가'].max() - df['저가'].min()) / df['고가'].max() * 100
        return ror, mdd, df


    def start(self):
        pass