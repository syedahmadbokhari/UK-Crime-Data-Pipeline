from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Core
    database_url: str = "sqlite:///./crimes.db"
    secret_key: str = "dev-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30

    # API
    api_prefix: str = "/api/v1"
    app_version: str = "2.0.0"

    # CORS
    allowed_origins: str = "http://localhost:5173,http://localhost:3000"

    # AI
    gemini_api_key: str = ""

    # Rate limiting
    rate_limit_default: str = "100/minute"
    rate_limit_expensive: str = "20/minute"

    # Redis caching
    redis_url: str = ""
    cache_ttl_seconds: int = 300

    # Observability
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "extra": "ignore"}

    @property
    def allowed_origins_list(self) -> list[str]:
        return [o.strip() for o in self.allowed_origins.split(",") if o.strip()]


settings = Settings()
