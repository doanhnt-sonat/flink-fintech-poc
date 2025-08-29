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
    Customer, Account, Transaction, Merchant, 
    CustomerSession,
    TransactionType, TransactionStatus, AccountType, CustomerTier,
    RiskLevel, Address, OutboxEvent, EventType
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
        
        # Base time for consistent data generation
        # Start with a fixed date but allow it to progress with each run
        self._initial_base_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)  # Initial: Noon on Jan 15, 2025
        self._base_time_progression = 0  # Track how many days to advance
        self.base_time = self._initial_base_time + timedelta(days=self._base_time_progression)
        
        # Pre-generated data for consistency
        self.merchants: List[Merchant] = []
        self.customers: List[Customer] = []
        self.accounts: List[Account] = []
        
        # Behavioral patterns
        self.transaction_patterns = {
            CustomerTier.BASIC: {'daily_txns': (1, 5), 'avg_amount': (10, 500)},
            CustomerTier.PREMIUM: {'daily_txns': (3, 15), 'avg_amount': (50, 2000)},
            CustomerTier.VIP: {'daily_txns': (5, 25), 'avg_amount': (100, 10000)},
            CustomerTier.ENTERPRISE: {'daily_txns': (10, 100), 'avg_amount': (1000, 100000)}
        }
        
        # Initialize base data
        self._generate_merchants(50)
    
    def advance_base_time(self, days: int = 1):
        """Advance the base time by specified number of days"""
        self._base_time_progression += days
        self.base_time = self._initial_base_time + timedelta(days=self._base_time_progression)
        print(f"🕐 Base time advanced by {days} day(s). New base time: {self.base_time}")
    
    def reset_base_time(self):
        """Reset base time to initial value"""
        self._base_time_progression = 0
        self.base_time = self._initial_base_time
        print(f"🔄 Base time reset to initial: {self.base_time}")
    
    def get_current_base_time(self) -> datetime:
        """Get current base time"""
        return self.base_time
    
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
        

        
        account = Account(
            customer_id=customer.id,
            account_number=f"ACC-{random.randint(100000000, 999999999)}",
            account_type=account_type,
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
                           transaction_type: Optional[TransactionType] = None,
                           session_start: Optional[datetime] = None,
                           session_end: Optional[datetime] = None) -> Transaction:
        """Generate a realistic transaction with behavioral patterns"""
        
        if not transaction_type:
            # Transaction type distribution based on account type
            if account.account_type == AccountType.CREDIT:
                # Credit accounts cannot have deposits, only payments and charges
                transaction_type = random.choices(
                    [TransactionType.CARD_PAYMENT, TransactionType.ATM_WITHDRAWAL, TransactionType.FEE_CHARGE],
                    weights=[80, 15, 5]
                )[0]
            elif account.account_type == AccountType.SAVINGS:
                # Savings accounts are primarily for deposits and transfers
                transaction_type = random.choices([
                    TransactionType.DEPOSIT, TransactionType.ACH_TRANSFER, 
                    TransactionType.INTERNAL_TRANSFER, TransactionType.INTEREST_PAYMENT
                ], weights=[40, 30, 20, 10])[0]
            elif account.account_type == AccountType.INVESTMENT:
                # Investment accounts for trading and dividends
                transaction_type = random.choices([
                    TransactionType.DEPOSIT, TransactionType.ACH_TRANSFER, 
                    TransactionType.INTEREST_PAYMENT, TransactionType.INTERNAL_TRANSFER
                ], weights=[30, 25, 25, 20])[0]
            else:
                # Checking and other accounts can have all types
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
        
        # Generate timestamp that's consistent with session if provided
        if session_start and session_end:
            # Transaction should occur during session time
            time_diff = (session_end - session_start).total_seconds()
            random_offset = random.uniform(0, time_diff)
            transaction_time = session_start + timedelta(seconds=random_offset)
        else:
            # If no session, create a realistic time sequence for the customer
            # Use base time as reference point for consistency
            # Random offset within last 24 hours from base time
            random_hours = random.uniform(0, 24)
            transaction_time = self.base_time - timedelta(hours=random_hours)
        
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
        
        # Override the default created_at timestamp
        transaction.created_at = transaction_time
        
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
    

    

    
    def generate_customer_session(self, customer: Customer) -> CustomerSession:
        """Generate customer session data with time boundaries"""
        # Use base time for consistency and reproducibility
        # Random offset within last 8 hours (480 minutes) from base time
        random_offset_minutes = random.randint(0, 480)
        started_at = self.base_time - timedelta(minutes=random_offset_minutes)
        session_duration = random.randint(30, 3600)  # 30 seconds to 1 hour
        ended_at = started_at + timedelta(seconds=session_duration)
        
        # Initial estimates - will be updated later with actual data
        estimated_transactions = random.randint(0, 5)  # Initial estimate
        base_actions = max(1, session_duration // 60)  # At least 1 action per minute
        transaction_actions = estimated_transactions * 3  # 3 actions per transaction
        other_actions = random.randint(5, 20)  # Other actions
        total_actions = base_actions + transaction_actions + other_actions
        
        return CustomerSession(
            customer_id=customer.id,
            session_id=str(uuid.uuid4()),
            channel=random.choice(['web', 'mobile', 'atm']),
            device_type=random.choice(['desktop', 'mobile', 'tablet', 'atm']),
            ip_address=self.fake.ipv4(),
            location=self._generate_transaction_location(),
            started_at=started_at,
            ended_at=ended_at,
            actions_count=total_actions,
            transactions_count=estimated_transactions
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
            'sessions': [],
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
            
            # Generate customer session FIRST to establish time boundaries
            session = None
            if random.random() > 0.3:  # 70% chance of having a session
                session = self.generate_customer_session(customer)
                scenario_data['sessions'].append(session)
            
            # Generate transactions for each account within session time if available
            customer_transactions = []  # Collect transactions for this customer first
            
            for account in customer_accounts:
                daily_txns = random.randint(*self.transaction_patterns[customer.tier]['daily_txns'])
                
                for _ in range(daily_txns):
                    # Pass session time boundaries to ensure transactions occur during session
                    transaction = self.generate_transaction(
                        customer, 
                        account,
                        session_start=session.started_at if session else None,
                        session_end=session.ended_at if session else None
                    )
                    customer_transactions.append(transaction)
            
            # Sort transactions by time to ensure logical sequence
            customer_transactions.sort(key=lambda x: x.created_at)
            
            # Add sorted transactions to scenario
            scenario_data['transactions'].extend(customer_transactions)
            
            # Generate outbox events for all transactions
            for transaction in customer_transactions:
                # Generate outbox event for transaction
                event = self.generate_outbox_event(
                    'Transaction',
                    transaction.id,
                    EventType.TRANSACTION_INITIATED if transaction.status == TransactionStatus.PENDING 
                    else EventType.TRANSACTION_COMPLETED,
                    transaction.dict()
                )
                scenario_data['outbox_events'].append(event)
            
            # Update session with actual transaction count after all transactions are created
            if session:
                # Recalculate session data based on actual transactions
                session_transactions = [
                    t for t in scenario_data['transactions'] 
                    if t.customer_id == customer.id and 
                    session.started_at <= t.created_at <= session.ended_at
                ]
                session.transactions_count = len(session_transactions)
                
                # Recalculate actions count based on actual transactions
                base_actions = max(1, (session.ended_at - session.started_at).total_seconds() // 60)
                transaction_actions = session.transactions_count * 3
                other_actions = random.randint(5, 20)
                session.actions_count = int(base_actions + transaction_actions + other_actions)
        
        # Automatically advance base time for next run to simulate time progression
        # This ensures each run generates data for a different day
        self.advance_base_time(days=1)
        
        return scenario_data
