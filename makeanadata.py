import os, environ
from pathlib import Path
import OpenDartReader
from pykrx import stock

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
            df = stock.get_market_fundamental("20210108")
            parent.print_tb(" data ", str(df))

        self.start()

    def start(self):
        pass