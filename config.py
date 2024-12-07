from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ENV: str
    PORT: int
    KAFKA_TOPIC: str
    KAFKA_CONSUMER_GROUP_ID: str
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_USERNAME: str
    KAFKA_PASSWORD: str
    AWS_S3_REGION: str
    AWS_S3_BUCKET: str
    AWS_S3_ACCESS_KEY: str
    AWS_S3_ACCESS_SECRET: str

    class Config:
        env_file = ".env"


settings = Settings()
