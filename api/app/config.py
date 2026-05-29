from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "sqlite:///./crimes.db"
    secret_key: str = "dev-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30

    # AI report generation & RAG — set GEMINI_API_KEY in Railway Variables
    gemini_api_key: str = ""

    # Rate limiting (slowapi format: "N/period")
    rate_limit_default: str = "100/minute"
    rate_limit_expensive: str = "20/minute"

    # Redis caching — leave empty to disable caching gracefully
    redis_url: str = ""
    cache_ttl_seconds: int = 300

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
