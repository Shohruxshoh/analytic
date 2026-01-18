from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    KAFKA_TOPIC: str = "events_raw"
    KAFKA_GROUP_ID: str = "event-processor"

    MONGO_URI: str = "mongodb://mongo:27017"
    MONGO_DB: str = "analytics"

    CLICKHOUSE_HOST: str = "clickhouse"
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DB: str = "analytics"

    # Processing
    BATCH_SIZE: int = 1000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )


settings = Settings()
