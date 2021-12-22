import pandas as pd

class strategy_parent:
    def __init__(self):
        self.df = pd.read_csv("data/fin_statement_new.csv")

        self.df = self.df.drop(["상장일"], axis=1)
        self.df = self.df.rename(columns={
            "DPS(보통주, 현금+주식, 연간)": "DPS",
            "P/E(Adj., FY End)": "PER",
            "P/B(Adj., FY End)": "PBR",
            "P/S(Adj., FY End)": "PSR",
        })

        # calculate year returns
        yearly_price_df = self.df.pivot(index="year", columns="Name", values="수정주가")
        self.yearly_rtn_df = yearly_price_df.pct_change(fill_method=None).shift(-1)