"""运行配置：端口、跨域、运行环境。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    app_name: str = "多源异构数据接入引擎"
    env: str = "local"
    port: int = 8000
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200
    # 解析-去噪流水线的 SQLite 落库位置；允许用环境变量指到临时库做端到端验证。
    pipeline_db_path: str = field(
        default_factory=lambda: os.environ.get("PIPELINE_DB_PATH", "data/pipeline.sqlite3")
    )


settings = Settings()
