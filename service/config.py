import os
from dotenv import load_dotenv

load_dotenv()  # 从 project/.env 加载

class Settings:
    # OKX
    # OKX_PUBLIC_URL: str = "wss://wspap.okx.com:8443/ws/v5/public"
    OKX_PRIVATE_URL: str = "wss://wspap.okx.com:8443/ws/v5/private"
    OKX_PUBLIC_URL: str = "wss://wspap.okx.com:8443/ws/v5/business"
    OKX_API_KEY: str = os.getenv("OKX_API_KEY")
    OKX_SECRET:  str = os.getenv("OKX_API_SECRET")
    OKX_PASSPHRASE: str = os.getenv("OKX_PASSPHRASE")


    # Postgres
    PG_DB: str = os.getenv("PG_DB")
    PG_USER: str = os.getenv("PG_USER")
    PG_PASSWORD: str = os.getenv("PG_PASSWORD")
    PG_HOST: str = os.getenv("PG_HOST")
    PG_PORT: int = int(os.getenv("PG_PORT", 5432))

settings = Settings()
