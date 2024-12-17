from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List
import json

class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file="settings.env", env_file_encoding="utf-8")

    kafka_broker_address: str
    kafka_topic: str
    pairs:List[str]
    
    @property
    def parsed_pairs(self) -> List[str]:
        return json.loads(self.pairs) if isinstance(self.pairs, str) else self.pairs

config = Config()

# print(config.parsed_pairs)

