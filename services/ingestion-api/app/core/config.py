from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    SERVICE_NAME: str = "ingestion-api"

    KAFKA_BOOTSTRAP_SERVERS: str = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    KAFKA_TOPIC: str = "events_raw"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )


settings = Settings()
