import os
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class HttpCfg(BaseModel):
    user_agent: str = Field(default="real-estate-scrapper/0.1")
    rate_limit_rps: float = Field(default=0.3, ge=0.05, le=5.0)
    timeout_s: int = Field(default=20, ge=1, le=120)
    http_proxy: str | None = None
    https_proxy: str | None = None

class IoCfg(BaseModel):
    out_dir: Path = Path("./data/out")
    img_dir: Path = Path("./data/images")

class LogCfg(BaseModel):
    level: str = Field(default="INFO")

class DefaultsCfg(BaseModel):
    city: str = "Gdańsk"
    deal: str = "sprzedaz"
    kind: str = "mieszkanie"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)
    http: HttpCfg = HttpCfg()
    io: IoCfg = IoCfg()
    log: LogCfg = LogCfg()
    defaults: DefaultsCfg = DefaultsCfg()

def ensure_dirs(cfg: Settings) -> None:
    cfg.io.out_dir.mkdir(parents=True, exist_ok=True)
    cfg.io.img_dir.mkdir(parents=True, exist_ok=True)

def load_settings() -> Settings:
    # pydantic-settings sam odczyta .env, ale tworzymy katalogi od razu
    s = Settings(
        http=HttpCfg(
            user_agent=os.getenv("USER_AGENT", HttpCfg().user_agent),
            rate_limit_rps=float(os.getenv("RATE_LIMIT_RPS", HttpCfg().rate_limit_rps)),
            timeout_s=int(os.getenv("HTTP_TIMEOUT_S", HttpCfg().timeout_s)),
            http_proxy=os.getenv("HTTP_PROXY") or None,
            https_proxy=os.getenv("HTTPS_PROXY") or None,
        ),
        io=IoCfg(
            out_dir=Path(os.getenv("OUT_DIR", IoCfg().out_dir)),
            img_dir=Path(os.getenv("IMG_DIR", IoCfg().img_dir)),
        ),
        log=LogCfg(level=os.getenv("LOG_LEVEL", LogCfg().level)),
        defaults=DefaultsCfg(
            city=os.getenv("DEFAULT_CITY", DefaultsCfg().city),
            deal=os.getenv("DEFAULT_DEAL", DefaultsCfg().deal),
            kind=os.getenv("DEFAULT_KIND", DefaultsCfg().kind),
        ),
    )
    ensure_dirs(s)
    return s
