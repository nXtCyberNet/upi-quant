"""
Pydantic models for transactions, users, and devices.
Shared across the entire backend.
"""

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from enum import Enum
import uuid


# ── Enums ────────────────────────────────────────────────────

class TransactionChannel(str, Enum):
    UPI = "UPI"
    NEFT = "NEFT"
    IMPS = "IMPS"


class IPASNType(str, Enum):
    MOBILE_ISP = "MOBILE_ISP"
    BROADBAND = "BROADBAND"
    ENTERPRISE = "ENTERPRISE"
    INDIAN_CLOUD = "INDIAN_CLOUD"
    HOSTING = "HOSTING"
    FOREIGN = "FOREIGN"
    UNKNOWN = "UNKNOWN"


class TransactionStatus(str, Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FLAGGED = "FLAGGED"
    BLOCKED = "BLOCKED"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


# ── Transaction Models ───────────────────────────────────────

class TransactionInput(BaseModel):
    """Incoming transaction payload from UPI gateway / simulator."""
    tx_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    sender_id: str
    receiver_id: str
    amount: float = Field(gt=0)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    device_hash: str
    device_os: Optional[str] = None
    device_model: Optional[str] = None
    device_is_emulator: Optional[bool] = None
    ip_address: Optional[str] = None
    ip_asn: Optional[str] = None
    ip_asn_type: IPASNType = IPASNType.UNKNOWN
    sim_verified: Optional[bool] = None
    sender_lat: Optional[float] = None
    sender_lon: Optional[float] = None
    channel: TransactionChannel = TransactionChannel.UPI
    upi_id_sender: Optional[str] = None
    upi_id_receiver: Optional[str] = None


class TransactionResult(BaseModel):
    """Full processing result returned to the caller."""
    tx_id: str
    sender_id: str
    receiver_id: str
    amount: float
    timestamp: datetime
    risk_score: float
    risk_level: RiskLevel
    status: TransactionStatus
    processing_time_ms: float
    breakdown: dict
    flags: List[str] = []
    cluster_id: Optional[str] = None


# ── User / Device Models ────────────────────────────────────

class UserProfile(BaseModel):
    """User behavioural profile stored on the :User node.

    Focuses on behavioral anchors for fraud detection.
    Balance removed: transient banking-layer value (NPCI auth phase).
    total_inflow removed: use windowed Cypher queries instead.
    Compliant with DPDP Act — no Sensitive Personal Data stored.
    """
    user_id: str
    upi_id: Optional[str] = None
    avg_tx_amount: float = 0.0
    std_tx_amount: float = 0.0
    tx_count: int = 0
    total_outflow: float = 0.0
    last_active: Optional[datetime] = None
    is_dormant: bool = False
    risk_score: float = 0.0
    last_lat: Optional[float] = None
    last_lon: Optional[float] = None
    city: Optional[str] = None
    kyc_status: str = "VERIFIED"


class DeviceInfo(BaseModel):
    """Device fingerprint information stored on the :Device node."""
    device_hash: str
    os: Optional[str] = None
    model: Optional[str] = None
    screen_resolution: Optional[str] = None
    is_emulator: bool = False
    device_score: float = 0.0
    account_count: int = 0
