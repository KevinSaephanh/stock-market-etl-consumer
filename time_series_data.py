from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class TimeSeriesData:
    time_series: Dict[str, Dict[str, Any]]  
    timeframe: str