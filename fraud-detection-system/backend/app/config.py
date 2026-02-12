"""
Application configuration.
Loads settings from environment variables with sensible defaults.
"""

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Central configuration for the Fraud Detection Engine."""

    # ── Application ─────────────────────────────────────────
    APP_NAME: str = "Real-Time Mule & Collusive Fraud Intelligence Engine"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = True
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # ── Neo4j ───────────────────────────────────────────────
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "password123"
    NEO4J_DATABASE: str = "neo4j"
    NEO4J_MAX_POOL_SIZE: int = 50

    # ── Redis ───────────────────────────────────────────────
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_STREAM_KEY: str = "transactions"
    REDIS_CONSUMER_GROUP: str = "fraud_workers"
    REDIS_ALERTS_CHANNEL: str = "fraud_alerts"

    # ── Worker Pool ─────────────────────────────────────────
    WORKER_COUNT: int = 4
    WORKER_BATCH_SIZE: int = 10

    # ── Graph Analytics (batch interval) ────────────────────
    GRAPH_ANALYTICS_INTERVAL_SEC: int = 5

    # ── Risk Fusion Weights ─────────────────────────────────
    WEIGHT_GRAPH: float = 0.30
    WEIGHT_BEHAVIORAL: float = 0.25
    WEIGHT_DEVICE: float = 0.20
    WEIGHT_DEAD_ACCOUNT: float = 0.15
    WEIGHT_VELOCITY: float = 0.10

    # ── Risk Thresholds ─────────────────────────────────────
    HIGH_RISK_THRESHOLD: float = 70.0
    MEDIUM_RISK_THRESHOLD: float = 40.0

    # ── Feature Parameters ──────────────────────────────────
    MMDB_PATH: str = "asn_ipv4_small.mmdb/asn_ipv4_small.mmdb"
    DORMANT_DAYS_THRESHOLD: int = 30
    DEVICE_ACCOUNT_THRESHOLD: int = 5
    VELOCITY_WINDOW_SEC: int = 60
    BEHAVIORAL_HISTORY_COUNT: int = 25
    PASS_THROUGH_RATIO_THRESHOLD: float = 0.80
    BURST_TX_THRESHOLD: int = 10
    IMPOSSIBLE_TRAVEL_KMH: float = 250.0
    NIGHT_START_HOUR: int = 23
    NIGHT_END_HOUR: int = 5

    # ── Simulation ──────────────────────────────────────────
    SIMULATION_TPS: int = 500
    SIMULATION_TOTAL_TX: int = 10000
    SIMULATION_USER_COUNT: int = 500
    SIMULATION_MULE_RATIO: float = 0.08
    SIMULATION_DEVICE_COUNT: int = 300

    model_config = {"env_file": ".env", "case_sensitive": True}


settings = Settings()
