"""
Advanced fake data generator for fintech streaming data
Generates realistic, interconnected financial data with complex relationships
"""

import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import List, Dict, Optional, Tuple
from faker import Faker
import numpy as np

from models import (
    Customer, Account, Transaction, Merchant, FraudAlert, 
    ComplianceEvent, AccountBalance, CustomerSession, MarketData,
    TransactionType, TransactionStatus, AccountType, CustomerTier,
    RiskLevel, Address, OutboxEvent, EventType, StreamEvent
)


class AdvancedDataGenerator:
    """
    Advanced data generator that creates realistic fintech data with:
    - Complex relationships between entities
    - Realistic behavioral patterns
    - Time-based correlations
    - Risk-based scenarios
    """
    
    def __init__(self, seed: Optional[int] = None):
        self.fake = Faker(['en_US'])
        if seed:
            Faker.seed(seed)
            random.seed(seed)
            np.random.seed(seed)
        
        # Pre-generated data for consistency
        self.merchants: List[Merchant] = []
        self.customers: List[Customer] = []
        self.accounts: List[Account] = []
        self.market_symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'META', 'BRK.A', 'V', 'JNJ']
        
        # Behavioral patterns
        self.transaction_patterns = {
            CustomerTier.BASIC: {'daily_txns': (1, 5), 'avg_amount': (10, 500)},
            CustomerTier.PREMIUM: {'daily_txns': (3, 15), 'avg_amount': (50, 2000)},
            CustomerTier.VIP: {'daily_txns': (5, 25), 'avg_amount': (100, 10000)},
            CustomerTier.ENTERPRISE: {'daily_txns': (10, 100), 'avg_amount': (1000, 100000)}
        }
        
        # Initialize base data
        self._generate_merchants(50)
    
    def _generate_merchants(self, count: int):
        """Generate a pool of merchants for transactions"""
        business_types = [
            'Restaurant', 'Gas Station', 'Grocery Store', 'Department Store',
            'Online Retailer', 'Pharmacy', 'Hotel', 'Airline', 'Bank',
            'Insurance', 'Utility', 'Subscription Service', 'Healthcare'
        ]
        
        mcc_codes = {
            'Restaurant': '5812', 'Gas Station': '5541', 'Grocery Store': '5411',
            'Department Store': '5311', 'Online Retailer': '5999', 'Pharmacy': '5912',
            'Hotel': '7011', 'Airline': '4511', 'Bank': '6011',
            'Insurance': '6300', 'Utility': '4900', 'Subscription Service': '5968',
            'Healthcare': '8011'
        }
        
        for _ in range(count):
            business_type = random.choice(business_types)
            merchant = Merchant(
                name=self.fake.company(),
                business_type=business_type,
                mcc_code=mcc_codes[business_type],
                address=self._generate_address(),
                phone=self.fake.phone_number(),
                website=self.fake.url(),
                tax_id=self.fake.ssn(),
                is_active=random.choice([True] * 9 + [False])  # 90% active
            )
            self.merchants.append(merchant)
    
    def _generate_address(self) -> Address:
        """Generate a realistic address"""
        return Address(
            street=self.fake.street_address(),
            city=self.fake.city(),
            state=self.fake.state_abbr(),
            zip_code=self.fake.zipcode(),
            country="US"
        )
    
    def generate_customer(self, tier: Optional[CustomerTier] = None) -> Customer:
        """Generate a realistic customer with behavioral characteristics"""
        if not tier:
            # Weighted distribution: 60% Basic, 25% Premium, 10% VIP, 5% Enterprise
            tier = random.choices(
                [CustomerTier.BASIC, CustomerTier.PREMIUM, CustomerTier.VIP, CustomerTier.ENTERPRISE],
                weights=[60, 25, 10, 5]
            )[0]
        
        # Risk score based on tier and randomness
        base_risk = {
            CustomerTier.BASIC: 20,
            CustomerTier.PREMIUM: 15,
            CustomerTier.VIP: 10,
            CustomerTier.ENTERPRISE: 5
        }
        
        risk_score = max(0, min(1000, 
            base_risk[tier] + random.gauss(0, 10)))
        
        customer = Customer(
            first_name=self.fake.first_name(),
            last_name=self.fake.last_name(),
            email=self.fake.email(),
            phone=self.fake.phone_number(),
            date_of_birth=self.fake.date_of_birth(minimum_age=18, maximum_age=80),
            ssn=self.fake.ssn(),
            address=self._generate_address(),
            tier=tier,
            risk_score=risk_score,
            kyc_status=random.choices(['verified', 'pending', 'rejected'], weights=[80, 15, 5])[0],
            credit_score=random.randint(300, 850) if random.random() > 0.1 else None,
            annual_income=Decimal(str(random.randint(25000, 500000))) if random.random() > 0.2 else None,
            employment_status=random.choice(['employed', 'self-employed', 'unemployed', 'retired', 'student']),
            onboarding_channel=random.choices(['web', 'mobile', 'branch', 'partner'], weights=[40, 35, 15, 10])[0],
            referral_code=self.fake.lexify(text='REF????') if random.random() > 0.7 else None,
            preferences={
                'notifications': random.choice([True, False]),
                'marketing': random.choice([True, False]),
                'paperless': random.choice([True, False])
            },
            tags=random.sample(['high-value', 'frequent-traveler', 'tech-savvy', 'price-sensitive', 'loyalty-member'], 
                             k=random.randint(0, 3))
        )
        
        self.customers.append(customer)
        return customer
    
    def generate_account(self, customer: Customer, account_type: Optional[AccountType] = None) -> Account:
        """Generate a bank account for a customer"""
        if not account_type:
            # Account type distribution based on customer tier
            if customer.tier == CustomerTier.ENTERPRISE:
                account_type = random.choices(
                    [AccountType.BUSINESS, AccountType.CHECKING, AccountType.INVESTMENT],
                    weights=[60, 30, 10]
                )[0]
            else:
                account_type = random.choices(
                    [AccountType.CHECKING, AccountType.SAVINGS, AccountType.CREDIT, AccountType.INVESTMENT],
                    weights=[50, 25, 20, 5]
                )[0]
        
        # Balance based on customer tier and account type
        balance_ranges = {
            CustomerTier.BASIC: (100, 5000),
            CustomerTier.PREMIUM: (1000, 50000),
            CustomerTier.VIP: (10000, 500000),
            CustomerTier.ENTERPRISE: (50000, 5000000)
        }
        
        min_bal, max_bal = balance_ranges[customer.tier]
        if account_type == AccountType.CREDIT:
            balance = Decimal(str(random.uniform(-max_bal * 0.3, 0)))  # Credit accounts have negative balance
            credit_limit = Decimal(str(random.uniform(1000, max_bal)))
        else:
            balance = Decimal(str(random.uniform(min_bal, max_bal)))
            credit_limit = None
        
        account = Account(
            customer_id=customer.id,
            account_number=f"ACC-{random.randint(100000000, 999999999)}",
            account_type=account_type,
            balance=balance,
            available_balance=balance * Decimal('0.95'),  # 95% available
            credit_limit=credit_limit,
            interest_rate=Decimal(str(random.uniform(0.01, 0.05))) if account_type == AccountType.SAVINGS else None,
            overdraft_protection=random.choice([True, False]),
            minimum_balance=Decimal('100') if account_type == AccountType.SAVINGS else Decimal('0'),
            monthly_fee=Decimal('0') if customer.tier in [CustomerTier.VIP, CustomerTier.ENTERPRISE] else Decimal(str(random.uniform(0, 25))),
            branch_code=f"BR{random.randint(1000, 9999)}",
            routing_number=f"{random.randint(100000000, 999999999)}"
        )
        
        self.accounts.append(account)
        return account
    
    def generate_transaction(self, customer: Customer, account: Account, 
                           transaction_type: Optional[TransactionType] = None) -> Transaction:
        """Generate a realistic transaction with behavioral patterns"""
        
        if not transaction_type:
            # Transaction type distribution based on account type and time
            if account.account_type == AccountType.CREDIT:
                transaction_type = random.choices(
                    [TransactionType.CARD_PAYMENT, TransactionType.CASH_ADVANCE, TransactionType.FEE_CHARGE],
                    weights=[80, 15, 5]
                )[0]
            else:
                transaction_type = random.choices([
                    TransactionType.CARD_PAYMENT, TransactionType.WIRE_TRANSFER, 
                    TransactionType.ACH_TRANSFER, TransactionType.ATM_WITHDRAWAL,
                    TransactionType.DEPOSIT, TransactionType.MOBILE_PAYMENT
                ], weights=[35, 15, 20, 15, 10, 5])[0]
        
        # Amount based on customer tier and transaction type
        tier_patterns = self.transaction_patterns[customer.tier]
        min_amount, max_amount = tier_patterns['avg_amount']
        
        # Transaction type modifiers
        type_modifiers = {
            TransactionType.CARD_PAYMENT: (0.5, 2.0),
            TransactionType.WIRE_TRANSFER: (2.0, 10.0),
            TransactionType.ATM_WITHDRAWAL: (0.1, 0.5),
            TransactionType.DEPOSIT: (1.0, 5.0),
            TransactionType.MOBILE_PAYMENT: (0.1, 1.0)
        }
        
        if transaction_type in type_modifiers:
            low_mod, high_mod = type_modifiers[transaction_type]
            min_amount *= low_mod
            max_amount *= high_mod
        
        amount = Decimal(str(round(random.uniform(min_amount, max_amount), 2)))
        
        # Fee calculation
        fee_amount = Decimal('0.00')
        if transaction_type == TransactionType.WIRE_TRANSFER:
            fee_amount = Decimal('25.00')
        elif transaction_type == TransactionType.ATM_WITHDRAWAL and random.random() > 0.7:
            fee_amount = Decimal('3.50')
        
        # Risk scoring
        risk_factors = 0
        if amount > Decimal('10000'):
            risk_factors += 20
        if customer.risk_score > 50:
            risk_factors += 10
        if transaction_type == TransactionType.WIRE_TRANSFER:
            risk_factors += 15
        
        risk_score = min(100, risk_factors + random.uniform(0, 20))
        risk_level = RiskLevel.CRITICAL if risk_score > 80 else \
                    RiskLevel.HIGH if risk_score > 60 else \
                    RiskLevel.MEDIUM if risk_score > 30 else RiskLevel.LOW
        
        # Select merchant for card payments
        merchant = random.choice(self.merchants) if transaction_type in [
            TransactionType.CARD_PAYMENT, TransactionType.MOBILE_PAYMENT
        ] else None
        
        # Transaction status based on risk
        if risk_score > 90:
            status = random.choices([TransactionStatus.PENDING, TransactionStatus.FAILED], weights=[30, 70])[0]
        elif risk_score > 70:
            status = random.choices([TransactionStatus.PENDING, TransactionStatus.COMPLETED], weights=[60, 40])[0]
        else:
            status = TransactionStatus.COMPLETED
        
        transaction = Transaction(
            transaction_type=transaction_type,
            status=status,
            from_account_id=account.id if transaction_type != TransactionType.DEPOSIT else None,
            to_account_id=account.id if transaction_type == TransactionType.DEPOSIT else None,
            customer_id=customer.id,
            amount=amount,
            fee_amount=fee_amount,
            description=self._generate_transaction_description(transaction_type, merchant),
            merchant_id=merchant.id if merchant else None,
            reference_number=f"TXN-{uuid.uuid4().hex[:12].upper()}",
            authorization_code=f"AUTH{random.randint(100000, 999999)}" if merchant else None,
            transaction_location=self._generate_transaction_location(),
            device_fingerprint=f"FP{uuid.uuid4().hex[:16]}",
            ip_address=self.fake.ipv4(),
            user_agent=self.fake.user_agent(),
            risk_score=risk_score,
            risk_level=risk_level,
            compliance_flags=self._generate_compliance_flags(risk_score, amount),
            processing_time_ms=random.randint(50, 5000),
            network=random.choice(['Visa', 'Mastercard', 'Amex', 'Discover']) if merchant else None,
            card_last_four=str(random.randint(1000, 9999)) if merchant else None,
            tags=self._generate_transaction_tags(transaction_type, risk_level),
            metadata={
                'channel': random.choice(['web', 'mobile', 'atm', 'pos']),
                'terminal_id': f"T{random.randint(10000, 99999)}" if merchant else None,
                'batch_id': f"B{random.randint(1000, 9999)}"
            }
        )
        
        return transaction
    
    def _generate_transaction_description(self, txn_type: TransactionType, merchant: Optional[Merchant]) -> str:
        """Generate realistic transaction description"""
        if merchant:
            return f"{merchant.name} - {merchant.business_type}"
        
        descriptions = {
            TransactionType.WIRE_TRANSFER: "Wire Transfer",
            TransactionType.ACH_TRANSFER: "ACH Transfer",
            TransactionType.ATM_WITHDRAWAL: "ATM Withdrawal",
            TransactionType.DEPOSIT: "Direct Deposit",
            TransactionType.FEE_CHARGE: "Monthly Maintenance Fee",
            TransactionType.INTEREST_PAYMENT: "Interest Payment"
        }
        
        return descriptions.get(txn_type, "Transaction")
    
    def _generate_transaction_location(self) -> Dict[str, float]:
        """Generate transaction location"""
        return {
            'latitude': float(self.fake.latitude()),
            'longitude': float(self.fake.longitude()),
            'city': self.fake.city(),
            'country': 'US'
        }
    
    def _generate_compliance_flags(self, risk_score: float, amount: Decimal) -> List[str]:
        """Generate compliance flags based on risk factors"""
        flags = []
        
        if amount > Decimal('10000'):
            flags.append('HIGH_VALUE')
        
        if risk_score > 70:
            flags.append('HIGH_RISK')
        
        if amount > Decimal('3000') and random.random() > 0.8:
            flags.append('STRUCTURING_SUSPECTED')
        
        if random.random() > 0.95:
            flags.append('SANCTIONS_CHECK_REQUIRED')
        
        return flags
    
    def _generate_transaction_tags(self, txn_type: TransactionType, risk_level: RiskLevel) -> List[str]:
        """Generate transaction tags"""
        tags = [txn_type.value, risk_level.value]
        
        if random.random() > 0.8:
            tags.extend(random.sample(['cross-border', 'recurring', 'first-time', 'bulk'], k=random.randint(1, 2)))
        
        return tags
    
    def generate_fraud_alert(self, customer: Customer, transaction: Optional[Transaction] = None) -> FraudAlert:
        """Generate a fraud alert"""
        alert_types = [
            'Unusual spending pattern', 'Geographic anomaly', 'Velocity check failed',
            'Device mismatch', 'Time-based anomaly', 'Amount threshold exceeded',
            'Merchant blacklist match', 'Card testing detected'
        ]
        
        alert = FraudAlert(
            customer_id=customer.id,
            transaction_id=transaction.id if transaction else None,
            alert_type=random.choice(alert_types),
            severity=random.choices([RiskLevel.LOW, RiskLevel.MEDIUM, RiskLevel.HIGH, RiskLevel.CRITICAL], 
                                  weights=[10, 30, 40, 20])[0],
            description=self.fake.text(max_nb_chars=200),
            confidence_score=random.uniform(0.5, 1.0),
            rules_triggered=[f"RULE_{random.randint(100, 999)}" for _ in range(random.randint(1, 3))]
        )
        
        return alert
    
    def generate_market_data(self) -> MarketData:
        """Generate realistic market data"""
        symbol = random.choice(self.market_symbols)
        base_price = random.uniform(50, 3000)
        change = random.gauss(0, base_price * 0.02)  # 2% volatility
        
        return MarketData(
            symbol=symbol,
            price=Decimal(str(round(base_price, 2))),
            change=Decimal(str(round(change, 2))),
            change_percent=Decimal(str(round((change / base_price) * 100, 2))),
            volume=random.randint(1000000, 100000000),
            market_cap=Decimal(str(random.randint(1000000000, 3000000000000)))
        )
    
    def generate_customer_session(self, customer: Customer) -> CustomerSession:
        """Generate customer session data"""
        started_at = datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 480))
        session_duration = random.randint(30, 3600)  # 30 seconds to 1 hour
        
        return CustomerSession(
            customer_id=customer.id,
            session_id=str(uuid.uuid4()),
            channel=random.choice(['web', 'mobile', 'atm']),
            device_type=random.choice(['desktop', 'mobile', 'tablet', 'atm']),
            ip_address=self.fake.ipv4(),
            location=self._generate_transaction_location(),
            started_at=started_at,
            ended_at=started_at + timedelta(seconds=session_duration),
            actions_count=random.randint(1, 50),
            transactions_count=random.randint(0, 5)
        )
    
    def generate_outbox_event(self, aggregate_type: str, aggregate_id: str, 
                            event_type: EventType, payload: Dict) -> OutboxEvent:
        """Generate outbox event for reliable messaging"""
        return OutboxEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload
        )
    
    def generate_realistic_scenario(self, num_customers: int = 10) -> Dict[str, List]:
        """Generate a complete realistic scenario with interconnected data"""
        scenario_data = {
            'customers': [],
            'accounts': [],
            'transactions': [],
            'fraud_alerts': [],
            'sessions': [],
            'market_data': [],
            'outbox_events': []
        }
        
        # Generate customers and their accounts
        for _ in range(num_customers):
            customer = self.generate_customer()
            scenario_data['customers'].append(customer)
            
            # Each customer has 1-4 accounts
            num_accounts = random.choices([1, 2, 3, 4], weights=[40, 35, 20, 5])[0]
            customer_accounts = []
            
            for _ in range(num_accounts):
                account = self.generate_account(customer)
                customer_accounts.append(account)
                scenario_data['accounts'].append(account)
            
            # Generate transactions for each account
            for account in customer_accounts:
                daily_txns = random.randint(*self.transaction_patterns[customer.tier]['daily_txns'])
                
                for _ in range(daily_txns):
                    transaction = self.generate_transaction(customer, account)
                    scenario_data['transactions'].append(transaction)
                    
                    # Generate outbox event for transaction
                    event = self.generate_outbox_event(
                        'Transaction',
                        transaction.id,
                        EventType.TRANSACTION_INITIATED if transaction.status == TransactionStatus.PENDING 
                        else EventType.TRANSACTION_COMPLETED,
                        transaction.dict()
                    )
                    scenario_data['outbox_events'].append(event)
                    
                    # Generate fraud alert for high-risk transactions
                    if transaction.risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL] and random.random() > 0.7:
                        alert = self.generate_fraud_alert(customer, transaction)
                        scenario_data['fraud_alerts'].append(alert)
            
            # Generate customer session
            if random.random() > 0.3:  # 70% chance of having a session
                session = self.generate_customer_session(customer)
                scenario_data['sessions'].append(session)
        
        # Generate market data
        for _ in range(10):
            market_data = self.generate_market_data()
            scenario_data['market_data'].append(market_data)
        
        return scenario_data
