from typing import List
import json
from websocket import create_connection
from .trade import Trade
from loguru import logger



class KrakenWebsocketAPI:

    URL = "wss://ws.kraken.com/v2"
    def __init__(self, pairs: List[str]):
        """
        Initialize the Kraken API.
        """
        self.pairs = pairs

        self.ws_client = create_connection(self.URL
        
        )
        logger.info('Connection established')

        self._subscribe(pairs)
    
    

    def get_trades(self) -> List[Trade]:
        """
        Get the recent trades for a given pair.
        """
        
        data = self.ws_client.recv()

        logger.debug(f"Received data: {data}")
        

        if 'heartbeat' in data:
            logger.info("Heartbeat received")
            return[]

        try:
            
            data = json.loads(data)
            logger.debug(f"Raw data: {data}")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
            
            return []
        

        try:
            trades_data= data['data']
        except KeyError as e:
            logger.error(f"No data field with trades in the message: {e}")
            return []

       
        trades = [
            Trade(
                pair=trade['symbol'],
                price=trade['price'],
                volume=trade['qty'],
                timestamp=trade['timestamp'],
                timestamp_ms=self.datestr2milliseconds(trade['timestamp']),
            ) for trade in trades_data
        ]

        return trades
       
    

    def _subscribe(self,pairs: List[str]):

        for pair in pairs:
            logger.info(f'Subscribing to trades for {pair}')

        
            subscription_message =json.dumps({
                "method": "subscribe",
                "params":{
                    "channel": "trade",
                    "symbol": [pair],
                    "snapshot": True
                }
            })

            self.ws_client.send(subscription_message)

            logger.info(f'Subscribed to trades for {pair}')




            for pair in self.pairs:
                _ = self.ws_client.recv()
                _ = self.ws_client.recv()

    def datestr2milliseconds(self, iso_time: str) -> int:
        """
        Convert ISO format datetime string to Unix milliseconds timestamp.
        """
        from datetime import datetime

        dt = datetime.strptime(iso_time, '%Y-%m-%dT%H:%M:%S.%fZ')
        return int(dt.timestamp() * 1000)
  

    
    

    