import pandas as pd
import numpy as np

import plothelper
from strategy_parent import strategy_parent


class strategy3(strategy_parent):
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
        filtered_df = self.df.join(market_cap_quantile_series, on="year", how="left", rsuffix="20%_quantile")
        filtered_df = filtered_df[filtered_df['시가총액'] <= filtered_df['시가총액20%_quantile']]

        pbr_rank_series = filtered_df.groupby("year")['PBR'].rank(method="max")
        per_rank_series = filtered_df.groupby("year")['PER'].rank(method="max")
        psr_rank_series = filtered_df.groupby("year")['PSR'].rank(method="max")

        filtered_df = filtered_df.join(pbr_rank_series, how="left", rsuffix="_rank")
        filtered_df = filtered_df.join(per_rank_series, how="left", rsuffix="_rank")
        filtered_df = filtered_df.join(psr_rank_series, how="left", rsuffix="_rank")

        self.parent.print_tb(" filtered_df", str(filtered_df))
        self.parent.print_tb(" rank에 nan값이 존재", str(filtered_df['PBR_rank'].isna().sum()))

        # nan을 0으로 채우기
        filtered_df.loc[:, filtered_df.filter(like="rank").columns] = filtered_df.filter(like="rank").fillna(0)

        filtered_df['rank_sum'] = filtered_df.filter(like="_rank").sum(axis=1)

        """
        Selector
        """
        max_rank_series = filtered_df.groupby("year")['rank_sum'].nlargest(15)
        selected_index = max_rank_series.index.get_level_values(1)

        selector_df = filtered_df.loc[selected_index].pivot(
            index='year', columns="Name", values="rank_sum"
        )

        asset_on_df = selector_df.notna().astype(int).replace(0, np.nan)
        selected_return_df = self.yearly_rtn_df * asset_on_df

        rtn_series, cum_rtn_series = plothelper.get_return_series(selected_return_df)
        plothelper.plot_return(cum_rtn_series, rtn_series)