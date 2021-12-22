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