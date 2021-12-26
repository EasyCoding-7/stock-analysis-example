import os, environ
import sqlite3
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
    def __init__(self, parent, using_open_dart=True):
        if using_open_dart:
            """
            ** 21-12-23 **
            * pykrx 사용하는 방향으로 선회
            * 더 필요한 데이터가 있다면 opendart에서 파싱하는 방향으로 변경예정
            ** 21-12-26 **
            * pykrx의 pbr데이터는 작년 pbr기준... 파싱이 필요하다
            * 그리고 이후에 필요한 데이터는 모두 opendart에 있기에 한 번은 해야할 작업
            """

            # read env file for opendart api
            base_dir = Path(__file__).resolve().parent
            environ.Env.read_env(
                env_file=os.path.join(base_dir, '.env')
            )

            # set opendart api
            api_key = env('OPENDART_API_KEY')
            dart = OpenDartReader(api_key)
            year_list = ['2019']

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
                    company_name = stock.get_market_ticker_name(ticker)
                    # print(종목)

                    # 'OOOO'년 회사의 3분기 보고서(11014)
                    this_year_3Q_fs = dart.finstate_all(corp=ticker, bsns_year=str(list), fs_div='CFS', reprt_code='11014')
                    # 'OOOO-1'년 회사의 사업보고서(11011)
                    last_year_fs = dart.finstate_all(corp=ticker, bsns_year=str(int(list)-1), fs_div='CFS', reprt_code='11011')

                    """
                    # 3분기 보고서
     rcept_no        reprt_code bsns_year corp_code sj_div sj_nm                                                account_id                                                                     account_nm     account_detail    thstrm_nm thstrm_amount frmtrm_nm  frmtrm_amount ord thstrm_add_amount frmtrm_q_nm frmtrm_q_amount frmtrm_add_amount
0    20191114002467  11014      2019      00365387  BS     재무상태표  ifrs-full_CurrentAssets                    유동자산           -                                                            제 20 기 3분기말  421173843404  제 19 기말   1511355997808  1   NaN               NaN         NaN             NaN             
1    20191114002467  11014      2019      00365387  BS     재무상태표  ifrs-full_CashAndCashEquivalents           현금및현금성자산       -                                                            제 20 기 3분기말  134553398888  제 19 기말   53509862897    2   NaN               NaN         NaN             NaN             
2    20191114002467  11014      2019      00365387  BS     재무상태표  ifrs-full_TradeAndOtherCurrentReceivables  매출채권 및 기타유동채권  -                                                            제 20 기 3분기말  101025292684  제 19 기말   80862783752    3   NaN               NaN         NaN             NaN             
3    20191114002467  11014      2019      00365387  BS     재무상태표  ifrs-full_Inventories                      재고자산           -                                                            제 20 기 3분기말  59194848631   제 19 기말   51456822550    4   NaN               NaN         NaN             NaN             
4    20191114002467  11014      2019      00365387  BS     재무상태표  ifrs-full_OtherCurrentFinancialAssets      기타유동금융자산       -                                                            제 20 기 3분기말  96462163103   제 19 기말   60948339838    5   NaN               NaN         NaN             NaN             
..              ...    ...       ...           ...  ..       ...                                    ...           ...      ..                                                                    ...          ...       ...           ...   ..   ...               ...         ...             ...             
178  20191114002467  11014      2019      00365387  SCE    자본변동표  ifrs-full_Equity                           기말자본           자본 [member]|지배기업의 소유주에게 귀속되는 자본 [member]|기타포괄손익누계액 [member]  제 20 기 3분기   -525008485    NaN       NaN            16  NaN               제 19 기 3분기  -299356066      NaN             
179  20191114002467  11014      2019      00365387  SCE    자본변동표  ifrs-full_Equity                           기말자본           자본 [member]|지배기업의 소유주에게 귀속되는 자본 [member]|이익잉여금 [member]      제 20 기 3분기   252195596487  NaN       NaN            16  NaN               제 19 기 3분기  158151595483    NaN             
180  20191114002467  11014      2019      00365387  SCE    자본변동표  ifrs-full_Equity                           기말자본           자본 [member]|지배기업의 소유주에게 귀속되는 자본 [member]|자본금 [member]        제 20 기 3분기   46822295000   NaN       NaN            16  NaN               제 19 기 3분기  46822295000     NaN             
181  20191114002467  11014      2019      00365387  SCE    자본변동표  ifrs-full_Equity                           기말자본           자본 [member]|지배기업의 소유주에게 귀속되는 자본 [member]|자본잉여금 [member]      제 20 기 3분기   106806482209  NaN       NaN            16  NaN               제 19 기 3분기  126753121445    NaN             
182  20191114002467  11014      2019      00365387  SCE    자본변동표  ifrs-full_Equity                           기말자본           자본 [member]|지배기업의 소유주에게 귀속되는 자본 [member]|자본조정                제 20 기 3분기  
                    """

                    """
# 사업보고서 
      rcept_no       reprt_code bsns_year corp_code sj_div sj_nm     account_id                                                             account_nm                                                                   account_detail thstrm_nm thstrm_amount frmtrm_nm  frmtrm_amount bfefrmtrm_nm bfefrmtrm_amount ord thstrm_add_amount
0    20200330003824  11011      2019      00365387  BS     재무상태표  ifrs-full_CurrentAssets                                                유동자산           -                                                            제 20 기    501055395105  제 19 기    1512070831211  제 18 기       318877073839     1   NaN             
1    20200330003824  11011      2019      00365387  BS     재무상태표  ifrs-full_CashAndCashEquivalents                                       현금및현금성자산       -                                                            제 20 기    122986211759  제 19 기    53509862897    제 18 기       77402142843      2   NaN             
2    20200330003824  11011      2019      00365387  BS     재무상태표  dart_ShortTermDepositsNotClassifiedAsCashEquivalents                   단기금융상품         -                                                            제 20 기                  제 19 기                   제 18 기       1366172085       3   NaN             
3    20200330003824  11011      2019      00365387  BS     재무상태표  dart_CurrentFinancialAssetDesignationAsAtFairValueThroughProfitOrLoss  당기손익인식금융자산     -                                                            제 20 기                  제 19 기                   제 18 기       1404922800       4   NaN             
4    20200330003824  11011      2019      00365387  BS     재무상태표  ifrs-full_TradeAndOtherCurrentReceivables                              매출채권 및 기타유동채권  -                                                            제 20 기    104945550415  제 19 기    80862783752    제 18 기       122363215331     5   NaN             
..              ...    ...       ...           ...  ..       ...                                        ...                                        ... ..                                                               ...             ...     ...            ...       ...                ...    ..   ...             
205  20200330003824  11011      2019      00365387  SCE    자본변동표  ifrs-full_Equity                                                       기말자본           자본 [member]|지배기업의 소유주에게 귀속되는 자본 [member]|기타포괄손익누계액 [member]  제 20 기    -3055969672   제 19 기    -733914250     제 18 기       -86326079        19  NaN             
206  20200330003824  11011      2019      00365387  SCE    자본변동표  ifrs-full_Equity                                                       기말자본           자본 [member]|지배기업의 소유주에게 귀속되는 자본 [member]|이익잉여금 [member]      제 20 기    198771796598  제 19 기    157174672951   제 18 기       160276835162     19  NaN             
207  20200330003824  11011      2019      00365387  SCE    자본변동표  ifrs-full_Equity                                                       기말자본           자본 [member]|지배기업의 소유주에게 귀속되는 자본 [member]|자본금 [member]        제 20 기    46822295000   제 19 기    46822295000    제 18 기       46822295000      19  NaN             
208  20200330003824  11011      2019      00365387  SCE    자본변동표  ifrs-full_Equity                                                       기말자본           자본 [member]|지배기업의 소유주에게 귀속되는 자본 [member]|자본잉여금 [member]      제 20 기    100667329755  제 19 기    105883019120   제 18 기       103825938845     19  NaN             
209  20200330003824  11011      2019      00365387  SCE    자본변동표  ifrs-full_Equity                                                       기말자본           자본 [member]|지배기업의 소유주에게 귀속되는 자본 [member]|자본조정                제 20 기    -24721096254  제 19 기    -22058673120   제 18 기       -22058673120     19  NaN             

                    """

                    #parent.print_tb(f" 3분기보고서 ", str(this_year_3Q_fs))
                    #parent.print_tb(f" 사업보고서 ", str(last_year_fs))
                    #break

                    if type(this_year_3Q_fs) != type(None) and type(last_year_fs) != type(None):
                        try:
                            equity = int(this_year_3Q_fs.loc[this_year_3Q_fs['sj_div'].isin(['BS']) & this_year_3Q_fs['account_id'].isin(
                                ['ifrs-full_Equity']), 'thstrm_amount'].replace(",", ""))
                            # 당기부채(부채총계)
                            liability = int(this_year_3Q_fs.loc[this_year_3Q_fs['sj_div'].isin(['BS']) & this_year_3Q_fs['account_id'].isin(
                                ['ifrs-full_Liabilities']), 'thstrm_amount'].replace(",", ""))
                            # 자본 + 부채 = 자산총계
                            assets = equity + liability
                        except:
                            parent.print_tb(f" 예외발생 ", f"{company_name} is Error")
                            continue

                        if type(df) == type(None):
                            df = pd.DataFrame({'ticker': [ticker], '회사명': [company_name], '자본': [assets]})
                        else:
                            new_data = {'ticker': ticker, '회사명': company_name, '자본': assets}
                            df = df.append(new_data, ignore_index=True)

                        print(f"{company_name} is db in")

                        # for debug
                        cnt += 1
                        if cnt > 20:
                            break

                        # break
                    else:
                        parent.print_tb(f" 예외발생 ", f"{company_name} is None")

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
        if mdd == 100:
            ror = 0
        return ror, mdd, df


    def start(self):
        pass