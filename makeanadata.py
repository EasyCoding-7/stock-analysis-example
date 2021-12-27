import os, environ
import sqlite3
from pathlib import Path
import OpenDartReader
from pykrx import stock
import pandas as pd
import numpy as np
from marcap import marcap_data

env = environ.Env(
    # set casting, default value
    DEBUG=(bool, False)
)

class make_ana_data:
    def __init__(self, parent, using_open_dart=True):
        if using_open_dart:
            """
            ** 21-12-23 **
            * pykrx 사용하는 방향으로 선회
            * 더 필요한 데이터가 있다면 opendart에서 파싱하는 방향으로 변경예정
            ** 21-12-26 **
            * pykrx의 pbr데이터는 작년 pbr기준... 파싱이 필요하다
            * 그리고 이후에 필요한 데이터는 모두 opendart에 있기에 한 번은 해야할 작업
            ** 21-12-27 **
            * 데이터 파싱이 어려움(규격화가 되어있지 않음)
            * 그냥 퀀트킹 쓰는게 나을듯..
            * 나중에 더 좋은 방안이 있으면 진행
            """

            # read env file for opendart api
            base_dir = Path(__file__).resolve().parent
            environ.Env.read_env(
                env_file=os.path.join(base_dir, '.env')
            )

            # set opendart api
            api_key = env('OPENDART_API_KEY')
            dart = OpenDartReader(api_key)
            year_list = ['2021']

            # connect db
            con = sqlite3.connect("./stock.db")

            for list in year_list:
                tickers_kospi = stock.get_market_ticker_list(list+"1101", market="KOSPI")
                tickers_kosdaq = stock.get_market_ticker_list(list+"1101", market="KOSDAQ")
                # print(tickers_kospi)
                # print(tickers_kosdaq)

                tickers_all_stock = tickers_kospi + tickers_kosdaq
                df = None
                cnt = 0

                for ticker in tickers_all_stock:
                    # ticker = '001040'
                    company_name = stock.get_market_ticker_name(ticker)
                    print(company_name, ticker)

                    # 'OOOO'년 회사의 3분기 보고서(11014)
                    this_year_3Q_fs = dart.finstate_all(corp=ticker, bsns_year=str(list), fs_div='CFS', reprt_code='11014')
                    last_year_3Q_fs = dart.finstate_all(corp=ticker, bsns_year=str(int(list)-1), fs_div='CFS',reprt_code='11014')
                    # 'OOOO-1'년 회사의 사업보고서(11011)
                    last_year_fs = dart.finstate_all(corp=ticker, bsns_year=str(int(list)-1), fs_div='CFS', reprt_code='11011')

                    total_stocks = self.calculate_total_stocks(ticker, list+"1101")
                    # print("total stocks : ", total_stocks)

                    """
                    this_year_3Q_fs.to_excel('./this_year_3Q_fs.xlsx')
                    last_year_fs.to_excel('./last_year_fs.xlsx')
                    last_year_3Q_fs.to_excel('./last_year_3Q_fs.xlsx')
                    return
                    """

                    if type(this_year_3Q_fs) != type(None) and \
                            type(last_year_fs) != type(None) and \
                            type(last_year_3Q_fs) != type(None):
                        try:
                            assets = self.calculate_this_year_asset(this_year_3Q_fs)
                            profit = self.calculate_4q_to_3q_profit(this_year_3Q_fs, last_year_3Q_fs, last_year_fs)
                        except Exception as e:
                            parent.print_tb(f" 예외발생 ", f"{company_name} is Error : {e}")
                            continue

                        if type(df) == type(None):
                            df = pd.DataFrame({'ticker': [ticker], '회사명': [company_name], '자본': [assets], '순이익': [profit], '발행주식수': [total_stocks]})
                        else:
                            new_data = {'ticker': ticker, '회사명': company_name, '자본': assets, '순이익': profit, '발행주식수':total_stocks}
                            df = df.append(new_data, ignore_index=True)

                        print(f"{company_name} is db in")

                        # for debug
                        cnt += 1
                        if cnt > 20:
                            break

                        # break
                    else:
                        if type(this_year_3Q_fs) == type(None):
                            parent.print_tb(f" 예외발생 ", f"this_year_3Q_fs : {company_name} is None")
                            print(f" 예외발생 ", f"this_year_3Q_fs : {company_name} is None")
                        if type(last_year_fs) == type(None):
                            parent.print_tb(f" 예외발생 ", f"last_year_fs : {company_name} is None")
                            print(f" 예외발생 ", f"last_year_fs : {company_name} is None")
                        if type(last_year_3Q_fs) == type(None):
                            parent.print_tb(f" 예외발생 ", f"last_year_3Q_fs : {company_name} is None")
                            print(f" 예외발생 ", f"last_year_3Q_fs : {company_name} is None")

                df = df.set_index('ticker')
                parent.print_tb(f" inserted db ", str(df))
                df.to_sql(list, con, if_exists='replace')

            con.close()
        else:
            df = None
            buy_start_date = '1201'  # MMDD
            buy_end_date = '1201'
            trading_ticker_number=20
            total_ror_list = []
            total_mdd_list = []
            year_list = ['2010'+buy_start_date,
                         '2011'+buy_start_date,
                         '2012'+buy_start_date,
                         '2013'+buy_start_date,
                         '2014'+buy_start_date,
                         '2015'+buy_start_date,
                         '2016'+buy_start_date,
                         '2017'+buy_start_date,
                         '2018'+buy_start_date,
                         '2019'+buy_start_date,
                         '2020'+buy_start_date,
                         '2021'+buy_start_date]

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

                # Selector - 2 : OO개 종목 필터링
                smallest_pbr_series = filtered_df.groupby("year")['PBR'].nsmallest(trading_ticker_number*2)
                # parent.print_tb(f" 년도별 하위 {trading_ticker_number}개 필터링", str(smallest_pbr_series))

                total_ror = 0
                total_mdd = 0

                # 상장폐지일시 df가 empty
                cnt=0
                for ticker in smallest_pbr_series.index:
                    # print(ticker[1])
                    df = stock.get_market_ohlcv(str(list[:4]+buy_start_date), str(str(int(list[:4])+1)+buy_end_date), ticker[1])
                    ror, mdd, df = self.calculate_ror_mdd(df)
                    if df.empty == False:
                        # parent.print_tb(" df", str(df))
                        # parent.print_tb(f" {ticker[1]} ", "ror : " + str(ror) + " | mdd : " + str(mdd))
                        print(list, ticker[1])
                        total_ror += ror
                        total_mdd += mdd
                        cnt += 1
                        if cnt >= trading_ticker_number:
                            break
                total_ror = total_ror / trading_ticker_number
                total_mdd = total_mdd / trading_ticker_number
                total_ror_list.append(total_ror)
                total_mdd_list.append(total_mdd)
                parent.print_tb(f" {list[:4]} ", "ror : " + str(total_ror) + " | mdd : " + str(total_mdd))

            total_ror = np.prod(total_ror_list)
            total_mdd = np.mean(total_mdd_list)

            parent.print_tb(f" total ", "ror : " + str(total_ror) + " | mdd : " + str(total_mdd))

        self.start()

    def calculate_total_stocks(self, ticker, date):
        df = marcap_data(date, code=ticker)
        df = df.assign(Amount=df['Amount'].astype('int64'), Marcap=df['Marcap'].astype('int64'))
        return int(df['Marcap'] / df['Close'])


    def calculate_4q_to_3q_profit(self, this_year_3Q_fs, last_year_3Q_fs, last_year_fs):
        sj_div_list = ['SCE', 'CIS', 'IS']
        account_id_list = ['ifrs-full_ProfitLoss']

        # 올해 1~3분기 당기순이익
        profit_this_year_1_3Q = this_year_3Q_fs.loc[this_year_3Q_fs['sj_div'].isin(sj_div_list) &
                                                    this_year_3Q_fs['account_id'].isin(account_id_list),
                                                    # this_year_3Q_fs['account_detail'].isin(account_detail_list),
                                                    'thstrm_add_amount'].replace(",", "")

        profit_this_year_1_3Q = int(profit_this_year_1_3Q.iat[0])
        # print("올해 1~3분기 당기순이익 : ", profit_this_year_1_3Q)

        # 지난해 1~3분기 당기순이익
        profit_last_year_1_3Q = last_year_3Q_fs.loc[last_year_3Q_fs['sj_div'].isin(sj_div_list) &
                                                    last_year_3Q_fs['account_id'].isin(account_id_list),
                                                    'thstrm_add_amount'].replace(",", "")

        profit_last_year_1_3Q = int(profit_last_year_1_3Q.iat[0])
        # print("지난해 1~3분기 당기순이익 : ", profit_last_year_1_3Q)

        # 지난해 한해 당기순이익
        profit_last_year = last_year_fs.loc[last_year_fs['sj_div'].isin(sj_div_list) &
                                            last_year_fs['account_id'].isin(account_id_list),
                                            'thstrm_amount'].replace(",", "")

        profit_last_year = int(profit_last_year.iat[0])
        # print("지난해 한해 당기순이익 : ", profit_last_year)

        # 작년 4Q ~ 올해 3Q 당기순이익 계산
        profit_last_year_4Q = profit_last_year - profit_last_year_1_3Q
        return profit_last_year_4Q + profit_this_year_1_3Q
        # print("지난해 4Q ~ 올해 3Q 당기순이익 : ", profit)

    def calculate_this_year_asset(self, this_year_3Q_fs):
        equity = int(this_year_3Q_fs.loc[this_year_3Q_fs['sj_div'].isin(['BS']) & this_year_3Q_fs['account_id'].isin(
            ['ifrs-full_Equity']), 'thstrm_amount'].replace(",", ""))
        # 당기부채(부채총계)
        liability = int(this_year_3Q_fs.loc[this_year_3Q_fs['sj_div'].isin(['BS']) & this_year_3Q_fs['account_id'].isin(
            ['ifrs-full_Liabilities']), 'thstrm_amount'].replace(",", ""))
        # 자본 + 부채 = 자산총계
        return equity + liability


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
        if mdd == 100:
            ror = 0
        return ror, mdd, df


    def start(self):
        pass