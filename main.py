import sys, os                  # system import
from datetime import datetime

from PyQt5 import uic           # PyQt
from PyQt5 import QtGui
from PyQt5.QtCore import *
from PyQt5.QtWidgets import *
import pandas as pd
import FinanceDataReader as fdr
import pandas_datareader.data as web
import numpy as np

from makeanadata import make_ana_data
from marcap import marcap_data
import sqlite3

import settings
import strategy1
import strategy2
import strategy3

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    """ 절대경로를 넘겨준다. PyInstaller를 안쓸꺼라면 굳이 필요는 없다. """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)

form = resource_path("main.ui")
form_class = uic.loadUiType(form)[0]

def clear_textbrowser_decorator(func):
    def func_wrapper(self):
        mw.on_clicked_clear_tb_btn()
        func(self)
    return func_wrapper

class MainWindow(QMainWindow, form_class):

    def __init__(self):
        super().__init__()
        settings.set_df_max_column()
        self.setupUi(self)
        self.setStyleSheet(open("./style.qss", "r").read())

        # connect
        self.load_data_btn.clicked.connect(self.on_clicked_load_data_btn)
        self.make_ana_data_btn.clicked.connect(self.on_clicked_make_ana_data_btn)
        self.df_multiple_btn.clicked.connect(self.on_clicked_df_multiple_btn)
        self.clear_tb_btn.clicked.connect(self.on_clicked_clear_tb_btn)
        self.stategy_1_btn.clicked.connect(self.on_clicked_stategy_1_btn)
        self.stategy_2_btn.clicked.connect(self.on_clicked_stategy_2_btn)
        self.stategy_3_btn.clicked.connect(self.on_clicked_stategy_3_btn)
        self.write_sql_test.clicked.connect(self.on_clicked_write_sql_test)
        self.read_sql_test.clicked.connect(self.on_clicked_read_sql_test)

        self.add_column_btn.clicked.connect(self.on_clicked_add_column_btn)
        self.add_row_btn.clicked.connect(self.on_clicked_add_row_btn)
        self.del_row_btn.clicked.connect(self.on_clicked_del_row_btn)

    @clear_textbrowser_decorator
    def on_clicked_add_column_btn(self):
        con = sqlite3.connect("./stock.db")

        cursor = con.cursor()
        cursor.execute("ALTER TABLE '2021' ADD COLUMN 'new_column'")

        con.commit()
        con.close()

    @clear_textbrowser_decorator
    def on_clicked_add_row_btn(self):
        con = sqlite3.connect("./stock.db")

        cursor = con.cursor()
        cursor.execute("INSERT INTO '2021' VALUES('1', '2', 3, 4, 5)")

        con.commit()
        con.close()

    @clear_textbrowser_decorator
    def on_clicked_del_row_btn(self):
        con = sqlite3.connect("./stock.db")

        cursor = con.cursor()
        cursor.execute("DELETE FROM '2021' WHERE ticker='1'")

        con.commit()
        con.close()

    @clear_textbrowser_decorator
    def on_clicked_write_sql_test(self):
        start = datetime(2010, 1, 1)
        end = datetime(2016, 6, 12)
        df = web.DataReader("078930.KS", "yahoo", start, end)

        con = sqlite3.connect("./kospi.db")
        df.to_sql('078930', con, if_exists='replace')

        self.print_tb(" db is writed ", str(df))

        con.close()

    @clear_textbrowser_decorator
    def on_clicked_read_sql_test(self):
        con = sqlite3.connect("./kospi.db")
        readed_df = pd.read_sql("SELECT * FROM '078930'", con, index_col='Date')

        self.print_tb(" db is readed ", str(readed_df))

        con.close()

    @clear_textbrowser_decorator
    def on_clicked_stategy_1_btn(self):
        strategy1.strategy1(self)

    @clear_textbrowser_decorator
    def on_clicked_stategy_2_btn(self):
        strategy2.strategy2(self)

    @clear_textbrowser_decorator
    def on_clicked_stategy_3_btn(self):
        strategy3.strategy3(self)

    @clear_textbrowser_decorator
    def on_clicked_df_multiple_btn(self):
        a = pd.DataFrame([[1, 2], [3, np.nan, ], [5, 6]], columns=["a", "b"])
        b = pd.DataFrame([[1, 2], [3, 4, ], [5, 6]], columns=["a", "b"]) * 10

        self.print_tb("a is ", str(a))
        self.print_tb("b is ", str(b))
        self.print_tb("a*b ", str(a*b))

        a = pd.DataFrame([[1, 2], [3, np.nan, ], [5, 6]], columns=["a", "b"])
        b = pd.DataFrame([[1, 2, 3], [3, 4, 5], [5, 6, 7]], columns=["c", "b", "d"]) * 10
        self.print_tb(" new a is ", str(a))
        self.print_tb(" new b is ", str(b))
        self.print_tb(" a*b ", str(a*b))

        return_df = pd.DataFrame(
            [
                [np.nan, np.nan, 2],
                [3, np.nan, 3],
                [5, 6, np.nan],
            ],
            columns=["삼성", "현대", "SK"]
        )
        asset_on_df = pd.DataFrame(
            [
                [0, 1],
                [0, 1],
                [1, 0],
            ],
            columns=["삼성", "SK"]
        )
        return_df
        asset_on_df

    @clear_textbrowser_decorator
    def on_clicked_make_ana_data_btn(self):
        make_ana_data(self)
        """
        # using datareader
        # samsung_df = fdr.DataReader('005390', '2017-01-01', '2017-12-31')
        # self.print_tb("8", str(samsung_df))




        df_005930 = marcap_data('2021-01-21', code='005930')
        df_005930 = df_005930.assign(Amount=df_005930['Amount'].astype('int64'),
                                     Marcap=df_005930['Marcap'].astype('int64'))
        self.print_tb(" df_005930 ", str(df_005930))
        """

    @clear_textbrowser_decorator
    def on_clicked_load_data_btn(self):
        """
        데이터 로드
        """
        df = pd.read_csv("data/fin_statement_new.csv")

        self.print_tb("0", str(df))

        """
        데이터 정리
        """
        # 상장일 열 제거
        df = df.drop(["상장일"], axis=1)

        # rename 열
        df = df.rename(columns={
            "DPS(보통주, 현금+주식, 연간)": "DPS",
            "P/E(Adj., FY End)": "PER",
            "P/B(Adj., FY End)": "PBR",
            "P/S(Adj., FY End)": "PSR",
        })


        """
        정렬 및 데이터 분석
        """
        # 년도로 정렬하고 이름이 몇 개인지 카운트
        self.print_tb("1", str(df.groupby(['year'])['Name'].count()))

        # 이름으로 정렬하고 몇 년동안 있었는지 카운트
        self.print_tb("2", str(df.groupby(['Name'])['year'].count()))

        # code or name의 중복 체킹 방법1
        self.print_tb("3", str(df.groupby(['year'])['Name'].nunique().equals(df.groupby(['year'])['Code'].nunique())))

        # code or name의 중복 체킹 방법2
        self.print_tb("4", str(df.groupby(['year', 'Name'])['Code'].nunique()))
        self.print_tb("5", str(df.groupby(['year', 'Name'])['Code'].nunique().nunique()))

        # index를 df의 year로 하고 / column을 df의 Name / 값은 df의 수정주가로 설정
        yearly_price_df = df.pivot(index="year", columns="Name", values="수정주가")
        self.print_tb("6", str(yearly_price_df))

        # 한 해 수익률 구하기
        yearly_rtn_df = yearly_price_df.pct_change(fill_method=None).shift(-1)
        self.print_tb("7", str(yearly_rtn_df))

        # 상장폐지 종목 처리
        self.print_tb("8", str(yearly_price_df['AD모터스']))
        self.print_tb("9", str(yearly_price_df['AD모터스'].pct_change(fill_method=None).shift(-1)))

        # self.load_data_btn.setDisabled(True)

    def print_tb(self, print_log_name="0",  msg=""):
        self.log_textBrowser.moveCursor(QtGui.QTextCursor.End)
        if print_log_name == "0":
            self.log_textBrowser.insertPlainText("\n" + msg)
        else:
            self.log_textBrowser.insertPlainText("***************" + print_log_name + " *************** : \n")
            self.log_textBrowser.insertPlainText("\n" + msg)
            self.log_textBrowser.insertPlainText("\n\n *************** " + print_log_name + " *************** - end : \n\n")
        QApplication.processEvents(QEventLoop.ExcludeUserInputEvents)

    def on_clicked_clear_tb_btn(self):
        self.log_textBrowser.clear()

app = QApplication(sys.argv)
mw = MainWindow()
mw.show()
app.exec_()