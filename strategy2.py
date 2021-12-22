import pandas as pd
import numpy as np

import plothelper
from strategy_parent import strategy_parent


class strategy2(strategy_parent):
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
        # ROA >= 0.05
        filtered_df = self.df[self.df['ROA'] >= 0.05]

        # 부채비율 <= 0.5
        filtered_df['부채비율'] = filtered_df['비유동부채'] / filtered_df['자산총계']
        filtered_df = filtered_df[filtered_df['부채비율'] <= 0.5]
        self.parent.print_tb(" 필터링(ROA >= 0.05 && 부채비율 <= 0.5) 된 데이터", str(filtered_df))

        """
        Selector(위의 투자전략22 것 그대로)
        """
        filtered_df = filtered_df[filtered_df['PBR'] >= 0.2]

        smallest_pbr_series = filtered_df.groupby("year")['PBR'].nsmallest(15)
        selected_index = smallest_pbr_series.index.get_level_values(1)

        selector_df = filtered_df.loc[selected_index].pivot(
            index='year', columns="Name", values="PBR"
        )
        self.parent.print_tb(" selector_df", str(selector_df))

        asset_on_df = selector_df.notna().astype(int).replace(0, np.nan)
        selected_return_df = self.yearly_rtn_df * asset_on_df

        rtn_series, cum_rtn_series = plothelper.get_return_series(selected_return_df)
        plothelper.plot_return(cum_rtn_series, rtn_series)