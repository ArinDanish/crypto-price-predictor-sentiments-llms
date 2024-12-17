# Mock the Kraken API
from pydantic import BaseModel
from datetime import datetime
from typing import List
from time import sleep


class Trade(BaseModel):

    """ 
    A trade from the Kraken API.
    """
    pair: str
    price: float
    volume: float
    timestamp: datetime
    timestamp_ms: int


    def to_dict(self) -> dict:
       return self.model_dump_json()
    
    
    #    return {
    # #        "pair": self.pair,
    # #        "price": self.price,
    # #        "volume": self.volume,
    # #        "timestamp_ms": self.timestamp_ms,
    #             "timestamp": self.timestamp,
    #    }

class KrakenMockAPI:

    def __init__(self, pair: str):
        """
        Initialize the Kraken API.
        """
        self.pair = pair

    def get_trades(self) -> list[Trade]:

        mock_trades = [
            Trade(pair=self.pair, price=10000.0,volume=0.1,timestamp=datetime.now(), timestamp_ms=1234567890),
            Trade(pair=self.pair, price=10001.0, volume=0.2, timestamp=datetime.now(), timestamp_ms=1234567891)
            ]
        """
        Get the recent trades for a given pair.
        """

        
        sleep(1)
        # Mock data
        return mock_trades