import pandas as pd
import numpy as np

import plothelper
from strategy_parent import strategy_parent


class strategy1(strategy_parent):
    def __init__(self, parent):
        super().__init__()
        self.parent = parent
        self.start()

    def start(self):
        """
        Load data
        """
        self.parent.print_tb(" origin df", str(self.df))

        """
        Filter
        """
        market_cap_quantile_series = self.df.groupby("year")['시가총액'].quantile(.2)
        self.parent.print_tb(" 년도별 시가 총액 하위 20%", str(market_cap_quantile_series))

        filtered_df = self.df.join(market_cap_quantile_series, on="year", how="left", rsuffix="20%_quantile")
        filtered_df = filtered_df[filtered_df['시가총액'] <= filtered_df['시가총액20%_quantile']]
        self.parent.print_tb(" 시가 총액 하위 20%만 남기고 필터링", str(filtered_df))

        """
        Selector
        """
        # Selector - 1
        filtered_df = filtered_df[filtered_df['PBR'] >= 0.2]
        self.parent.print_tb(" PBR 0.2 이상 필터링", str(filtered_df))

        # Selector - 2
        smallest_pbr_series = filtered_df.groupby("year")['PBR'].nsmallest(15)
        self.parent.print_tb(" 년도별 하위 15개 필터링", str(smallest_pbr_series))

        # Selector - 3
            # 시가 총액 하위 20% + PBR 0.2 이상 + 년도별 하위 15개의 index를 넘긴다
        selected_index = smallest_pbr_series.index.get_level_values(1)

        selector_df = filtered_df.loc[selected_index].pivot(
            index='year', columns="Name", values="PBR"
        )
        # 필터링된 데이터는 pbr이 들어가고 나머지는 nan가 들어간다
        self.parent.print_tb(" selector_df", str(selector_df))

        # pbr이 있을경우 매수했다고 가정해서 1로 처리 + pbr이 0인 데이터는 nan로 처리
        asset_on_df = selector_df.notna().astype(int).replace(0, np.nan)
        self.parent.print_tb(" asset_on_df", str(asset_on_df))

        selected_return_df = self.yearly_rtn_df * asset_on_df

        rtn_series, cum_rtn_series = plothelper.get_return_series(selected_return_df)
        plothelper.plot_return(cum_rtn_series, rtn_series)
