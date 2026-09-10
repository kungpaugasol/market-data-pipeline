# ingestion/universe/loader.py
import pandas as pd
from ingestion.common.config import UNIVERSE_PATH

def get_active_universe() -> list[str]:
    df = pd.read_csv(UNIVERSE_PATH)
    return df.loc[df["active"], "symbol"].tolist()