# Import the Libraries:-
import pandas as pd
from sqlalchemy import create_engine

# Config Parameters:-
local = create_engine(f"mysql+pymysql://root:admin@127.0.0.1:3306/meredithdb")
stage = create_engine(
    f"mysql+pymysql://ksubramanian:UD89^EBQ_2rQQ7_^J*6s@127.0.0.1:3307/a3d3ac7f-0a70-45e3-9bf0-c916400c9923")
prod = create_engine(f"mysql+pymysql://tia:CT#gkARvQZe%+k?-Tw*,@127.0.0.1:3308/meredith_lodging_llc")

sql = """
        SELECT * FROM edi_src_hist_v12_escapia_reservation_report_ctrl_test;
"""
df = pd.read_sql(sql, con=prod)
df.to_sql("edi_src_hist_v12_escapia_reservation_report_ctrl_test", con=local, index=False, if_exists="replace")