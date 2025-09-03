"""
Complex data models for fintech streaming data
Includes relationships between customers, accounts, transactions, and various events
"""

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4
from pydantic import BaseModel, Field, validator
from dataclasses import dataclass
import json


class TransactionType(str, Enum):
    CARD_PAYMENT = "card_payment"
    WIRE_TRANSFER = "wire_transfer"
    ACH_TRANSFER = "ach_transfer"
    MOBILE_PAYMENT = "mobile_payment"
    ATM_WITHDRAWAL = "atm_withdrawal"
    DEPOSIT = "deposit"
    INTERNAL_TRANSFER = "internal_transfer"
    LOAN_DISBURSEMENT = "loan_disbursement"
    LOAN_PAYMENT = "loan_payment"
    FEE_CHARGE = "fee_charge"
    INTEREST_PAYMENT = "interest_payment"
    REFUND = "refund"


class TransactionStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    DISPUTED = "disputed"
    REVERSED = "reversed"


class AccountType(str, Enum):
    CHECKING = "checking"
    SAVINGS = "savings"
    CREDIT = "credit"
    LOAN = "loan"
    INVESTMENT = "investment"
    BUSINESS = "business"


class CustomerTier(str, Enum):
    BASIC = "basic"
    PREMIUM = "premium"
    VIP = "vip"
    ENTERPRISE = "enterprise"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class EventType(str, Enum):
    CUSTOMER_CREATED = "customer_created"
    CUSTOMER_UPDATED = "customer_updated"
    ACCOUNT_OPENED = "account_opened"
    ACCOUNT_CLOSED = "account_closed"
    TRANSACTION_CREATED = "transaction_created"
    TRANSACTION_INITIATED = "transaction_initiated"
    TRANSACTION_COMPLETED = "transaction_completed"
    TRANSACTION_FAILED = "transaction_failed"
    CUSTOMER_SESSION = "customer_session"
    FRAUD_DETECTED = "fraud_detected"
    LIMIT_EXCEEDED = "limit_exceeded"
    BALANCE_LOW = "balance_low"
    PAYMENT_DUE = "payment_due"
    COMPLIANCE_ALERT = "compliance_alert"


class BaseEntity(BaseModel):
    """Base model for all entities"""
    id: str = Field(default_factory=lambda: str(uuid4()))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = Field(default=1)


class Address(BaseModel):
    """Address model for customers and merchants"""
    street: str
    city: str
    state: str
    zip_code: str
    country: str = "US"
    
    @validator('zip_code')
    def validate_zip(cls, v):
        if len(v) not in [5, 10]:  # US zip codes
            raise ValueError('Invalid zip code format')
        return v


class Customer(BaseEntity):
    """Enhanced customer model with complex attributes"""
    first_name: str
    last_name: str
    email: str
    phone: str
    date_of_birth: datetime
    ssn: Optional[str] = None
    address: Address
    tier: CustomerTier = CustomerTier.BASIC
    risk_score: float = Field(ge=0, le=1000)
    kyc_status: str = "pending"  # pending, verified, rejected
    is_active: bool = True
    credit_score: Optional[int] = Field(None, ge=300, le=850)
    annual_income: Optional[Decimal] = None
    employment_status: Optional[str] = None
    onboarding_channel: str = "web"  # web, mobile, branch, partner
    referral_code: Optional[str] = None
    preferences: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)


class Account(BaseEntity):
    """Bank account model"""
    customer_id: str
    account_number: str
    account_type: AccountType
    currency: str = "USD"
    interest_rate: Optional[Decimal] = None
    is_active: bool = True
    is_frozen: bool = False
    overdraft_protection: bool = False
    minimum_balance: Decimal = Decimal('0.00')
    monthly_fee: Decimal = Decimal('0.00')
    branch_code: Optional[str] = None
    routing_number: Optional[str] = None


class Merchant(BaseEntity):
    """Merchant/business entity for transactions"""
    name: str
    business_type: str
    mcc_code: str  # Merchant Category Code
    address: Address
    phone: Optional[str] = None
    website: Optional[str] = None
    tax_id: Optional[str] = None
    is_active: bool = True


class Transaction(BaseEntity):
    """Complex transaction model with rich metadata"""
    transaction_type: TransactionType
    status: TransactionStatus = TransactionStatus.PENDING
    from_account_id: Optional[str] = None
    to_account_id: Optional[str] = None
    customer_id: str
    amount: Decimal
    currency: str = "USD"
    exchange_rate: Optional[Decimal] = None
    fee_amount: Decimal = Decimal('0.00')
    description: str
    merchant_id: Optional[str] = None
    reference_number: str
    authorization_code: Optional[str] = None
    
    # Location and device info
    transaction_location: Optional[Dict[str, Any]] = None
    device_fingerprint: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    
    # Risk and compliance
    risk_score: float = Field(ge=0, le=100)
    risk_level: RiskLevel = RiskLevel.LOW
    compliance_flags: List[str] = Field(default_factory=list)
    
    # Processing details
    processing_time_ms: Optional[int] = None
    network: Optional[str] = None  # Visa, Mastercard, etc.
    card_last_four: Optional[str] = None
    
    # Metadata
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @validator('amount', 'fee_amount')
    def validate_amount(cls, v):
        return round(v, 2)





class CustomerSession(BaseEntity):
    """Customer session tracking for behavioral analysis"""
    customer_id: str
    session_id: str
    channel: str  # web, mobile, atm, branch
    device_type: Optional[str] = None
    ip_address: Optional[str] = None
    location: Optional[Dict[str, Any]] = None
    started_at: datetime
    ended_at: Optional[datetime] = None
    actions_count: int = 0
    transactions_count: int = 0












