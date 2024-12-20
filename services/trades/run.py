from loguru import logger
from kraken_api.mock import KrakenMockAPI
from quixstreams import Application
from typing import Union
from kraken_api.websocket_api import KrakenWebsocketAPI


def main(
    kafka_broker_address: str,
    kafka_topic: str,
    kraken_api: Union[KrakenWebsocketAPI, KrakenMockAPI],
):
    """
    It does 2 things:
    1. Reads trades from Kraken Websocket API
    2. Pushes them in the given 'kafka topic'

    Args:
        kafka_broker_address (str) : The address of the Kafka broker
        kafka_topic (str) : The Kafka topic to save the trades
        kraken_api (Union[KrakenWebsocketAPI, KrakenMockAPI]) : Kraken API instance to get trades from

    Returns:
        None
    """
    logger.info("Start the trades service")

    app = Application(broker_address=kafka_broker_address)

    topic = app.topic(name=kafka_topic, value_serializer="json")

    with app.get_producer() as producer:
        while True:
            trades = kraken_api.get_trades()
            if not trades:
                # breakpoint()
                logger.warning("No trades received from Kraken API.")
                continue  # Skip to the next iteration if no trades are received

            for trade in trades:
                message = topic.serialize(key=trade.pair, value=trade.model_dump())

                producer.produce(topic=topic.name, value=message.value, key=message.key)

            logger.info(f"Pushed Trades to Kafka: {trades} trades")


if __name__ == "__main__":
    from config import config

    kraken_api = KrakenWebsocketAPI(pairs=config.pairs)

    main(
        kafka_broker_address=config.kafka_broker_address,
        kafka_topic=config.kafka_topic,
        kraken_api=kraken_api,
    )
