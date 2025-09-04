"""
Realtime data producer for fintech streaming data
Continuously generates and stores realistic financial data in PostgreSQL using direct CDC
"""

import asyncio
import json
import logging
import os
import random
import signal
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from decimal import Decimal
import uuid

import structlog
from enum import Enum
from psycopg2.extras import Json as PsycoJson

def _json_default_serializer(obj):
    """Serialize non-JSON types (datetime, Decimal, Enum, UUID) to JSON-friendly values."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, Enum):
        return obj.value
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

from models import (
    Customer, Account, Transaction, Merchant, 
    EventType, TransactionStatus,
    TransactionType, CustomerTier, RiskLevel, AccountType
)
from data_generator import AdvancedDataGenerator
from connectors import DatabaseManager

logger = structlog.get_logger()


class RealtimeDataProducer:
    """
    Realtime data producer that generates continuous streams of financial data
    Features:
    - Realistic transaction patterns based on time of day
    - Customer behavioral modeling
    - Risk-based event generation
    - Multi-threaded data generation
    - Graceful shutdown handling
    - Direct CDC: Data stored in PostgreSQL, streamed via Debezium
    """
    
    def __init__(self, 
                  db_connection_string: str,
                  production_rate: int = 10,  # transactions per second
                  num_customers: int = 100):
        
        self.db_manager = DatabaseManager(db_connection_string)
        # KafkaManager no longer needed - using direct CDC with Debezium
        self.data_generator = AdvancedDataGenerator()
        self.production_rate = production_rate
        self.num_customers = num_customers
        
        # Runtime state
        self.is_running = False
        self.customers: List[Customer] = []
        self.accounts: List[Account] = []
        self.customer_accounts: Dict[str, List[Account]] = {}
        self.merchants: List[Merchant] = []
        
        # Progressive time management
        # Load or initialize progressive time from file
        self.time_file = "progressive_time.json"
        self.current_base_time = self._load_progressive_time()
        self.last_time_update = None
        self.time_progression_interval = 10  # Update base time every 5 seconds
        
        # Initialize data generator's base time immediately
        self.data_generator.base_time = self.current_base_time
        
        # Statistics
        self.stats = {
            'transactions_generated': 0,
            'events_sent': 0,
            'errors': 0,
            'start_time': None
        }
        
        # Kafka topics from config (for reference - created by Debezium from tables)
        from config import config
        self.topics = config.KAFKA_TOPICS
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _load_progressive_time(self) -> datetime:
        """Load progressive time from file or use initial time"""
        try:
            if os.path.exists(self.time_file):
                with open(self.time_file, 'r') as f:
                    time_data = json.load(f)
                    saved_time = datetime.fromisoformat(time_data['base_time'])
                    logger.info("Loaded progressive time from file", 
                               saved_time=saved_time,
                               file=self.time_file)
                    return saved_time
            else:
                # Use initial time if file doesn't exist
                initial_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
                logger.info("No time file found, using initial time", 
                           initial_time=initial_time)
                return initial_time
        except Exception as e:
            logger.warning("Failed to load progressive time, using initial time", 
                          error=str(e))
            return datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    
    def _save_progressive_time(self):
        """Save current progressive time to file"""
        try:
            time_data = {
                'base_time': self.current_base_time.isoformat(),
                'last_updated': datetime.now(timezone.utc).isoformat(),
                'version': '1.0'
            }
            
            with open(self.time_file, 'w') as f:
                json.dump(time_data, f, indent=2)
            
            logger.info("Progressive time saved to file", 
                       time=self.current_base_time,
                       file=self.time_file)
        except Exception as e:
            logger.error("Failed to save progressive time", error=str(e))
    
    async def _update_base_time_if_needed(self):
        """Update base time to simulate real-time progression"""
        now = datetime.now(timezone.utc)
        
        if (self.last_time_update is None or 
            (now - self.last_time_update).total_seconds() >= self.time_progression_interval):
            
            # Advance base time by 1 day
            if self.current_base_time is None:
                self.current_base_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
            else:
                self.current_base_time += timedelta(days=1)
            
            # Update data generator's base time
            self.data_generator.base_time = self.current_base_time
            
            self.last_time_update = now
            
            # Save progressive time to file
            self._save_progressive_time()
            
            logger.info("Base time updated and saved", new_base_time=self.current_base_time)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("Received shutdown signal, stopping producer...", signal=signum)
        self.stop()
    
    async def initialize(self):
        """Initialize the producer with base data"""
        logger.info("Initializing realtime data producer...")
        
        # Check if database already has data by checking if customers table has any records
        has_existing_data = await self._check_database_has_data()
        
        if has_existing_data:
            logger.info("Database already has data, loading existing data...")
            # Load existing customers and accounts from database
            self.customers = await self._load_existing_customers()
            self.accounts = await self._load_existing_accounts()
            self.merchants = await self._load_existing_merchants()
            
            # Build customer -> accounts mapping
            for account in self.accounts:
                if account.customer_id not in self.customer_accounts:
                    self.customer_accounts[account.customer_id] = []
                self.customer_accounts[account.customer_id].append(account)
                
            logger.info("Loaded existing data", 
                       customers=len(self.customers),
                       accounts=len(self.accounts),
                       merchants=len(self.merchants))
        else:
            logger.info("Database is empty, initializing tables and generating sample data...")
            # Only initialize tables when we need to create new data
            self.db_manager.init_tables()
            
            # Generate initial customer, account and merchant data only (no sessions/transactions)
            scenario_data = self.data_generator.generate_realistic_scenario(self.num_customers)
            # Use the generator's internal merchant pool
            self.merchants = self.data_generator.merchants
            
            self.customers = scenario_data['customers']
            self.accounts = scenario_data['accounts']
            
            # Note: sessions and transactions will be generated in realtime
            
            # Build customer -> accounts mapping
            for account in self.accounts:
                if account.customer_id not in self.customer_accounts:
                    self.customer_accounts[account.customer_id] = []
                self.customer_accounts[account.customer_id].append(account)
            
            # Store initial data in database (including merchants)
            await self._store_initial_data()
            
            logger.info("Generated and stored new sample data", 
                       customers=len(self.customers),
                       accounts=len(self.accounts),
                       merchants=len(self.merchants))
    
    async def _check_database_has_data(self) -> bool:
        """Check if database already has data by checking if customers table has any records"""
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM customers")
                    count = cursor.fetchone()[0]
                    return count > 0
        except Exception as e:
            logger.warning("Failed to check database data, assuming empty", error=str(e))
            return False
    
    async def _load_existing_customers(self) -> List[Customer]:
        """Load existing customers from database"""
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, first_name, last_name, email, phone, date_of_birth,
                               ssn, address, tier, risk_score, kyc_status, is_active,
                               credit_score, annual_income, employment_status, 
                               onboarding_channel, referral_code, preferences, tags,
                               created_at, updated_at, version
                        FROM customers
                    """)
                    rows = cursor.fetchall()
                    customers = []
                    for row in rows:
                        try:
                            # Convert database row to Customer object
                            customer_data = {
                                'id': row[0],
                                'first_name': row[1],
                                'last_name': row[2],
                                'email': row[3],
                                'phone': row[4],
                                'date_of_birth': row[5],
                                'ssn': row[6],
                                'address': json.loads(row[7]) if row[7] else {},
                                'tier': CustomerTier(row[8]) if row[8] else CustomerTier.BASIC,
                                'risk_score': float(row[9]) if row[9] else 0.0,
                                'kyc_status': row[10],
                                'is_active': row[11],
                                'credit_score': row[12],
                                'annual_income': float(row[13]) if row[13] else 0.0,
                                'employment_status': row[14],
                                'onboarding_channel': row[15],
                                'referral_code': row[16],
                                'preferences': json.loads(row[17]) if row[17] else {},
                                'tags': json.loads(row[18]) if row[18] else [],
                                'created_at': row[19],
                                'updated_at': row[20],
                                'version': row[21] or 1
                            }
                            customers.append(Customer(**customer_data))
                        except Exception as e:
                            logger.warning("Failed to parse customer row", row=row, error=str(e))
                            continue
                    return customers
        except Exception as e:
            logger.error("Failed to load existing customers", error=str(e))
            raise
    
    async def _load_existing_accounts(self) -> List[Account]:
        """Load existing accounts from database"""
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, customer_id, account_number, account_type, currency,
                               balance, available_balance, credit_limit, interest_rate, 
                               is_active, is_frozen, overdraft_protection, 
                               minimum_balance, monthly_fee, branch_code, routing_number, 
                               created_at, updated_at, version
                        FROM accounts
                    """)
                    rows = cursor.fetchall()
                    accounts = []
                    for row in rows:
                        try:
                            # Convert database row to Account object
                            account_data = {
                                'id': row[0],
                                'customer_id': row[1],
                                'account_number': row[2],
                                'account_type': AccountType(row[3]) if row[3] else AccountType.CHECKING,
                                'currency': row[4],
                                'balance': float(row[5]) if row[5] else 0.0,
                                'available_balance': float(row[6]) if row[6] else 0.0,
                                'credit_limit': float(row[7]) if row[7] else 0.0,
                                'interest_rate': float(row[8]) if row[8] else 0.0,
                                'is_active': row[9],
                                'is_frozen': row[10],
                                'overdraft_protection': row[11],
                                'minimum_balance': float(row[12]) if row[12] else 0.0,
                                'monthly_fee': float(row[13]) if row[13] else 0.0,
                                'branch_code': row[14],
                                'routing_number': row[15],
                                'created_at': row[16],
                                'updated_at': row[17],
                                'version': row[18] or 1
                            }
                            accounts.append(Account(**account_data))
                        except Exception as e:
                            logger.warning("Failed to parse account row", row=row, error=str(e))
                            continue
                    return accounts
        except Exception as e:
            logger.error("Failed to load existing accounts", error=str(e))
            raise
    
    async def _store_initial_data(self):
        """Store initial customer, account and merchant data in database"""
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Store merchants first so foreign keys in transactions can reference them later
                    for merchant in self.merchants:
                        cursor.execute("""
                            INSERT INTO merchants (
                                id, name, business_type, mcc_code, address, phone, website,
                                tax_id, is_active, created_at, updated_at, version
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s
                            ) ON CONFLICT (id) DO NOTHING
                        """, (
                            merchant.id, merchant.name, merchant.business_type, merchant.mcc_code,
                            json.dumps(merchant.address.dict()), merchant.phone, merchant.website,
                            merchant.tax_id, merchant.is_active, merchant.created_at, merchant.updated_at,
                            merchant.version
                        ))
                    # Store customers
                    for customer in self.customers:
                        cursor.execute("""
                            INSERT INTO customers (
                                id, first_name, last_name, email, phone, date_of_birth,
                                ssn, address, tier, risk_score, kyc_status, is_active,
                                credit_score, annual_income, employment_status, 
                                onboarding_channel, referral_code, preferences, tags,
                                created_at, updated_at, version
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            ) ON CONFLICT (id) DO NOTHING
                        """, (
                            customer.id, customer.first_name, customer.last_name,
                            customer.email, customer.phone, customer.date_of_birth,
                            customer.ssn, json.dumps(customer.address.dict()),
                            customer.tier.value, customer.risk_score, customer.kyc_status,
                            customer.is_active, customer.credit_score, customer.annual_income,
                            customer.employment_status, customer.onboarding_channel,
                            customer.referral_code, json.dumps(customer.preferences),
                            json.dumps(customer.tags), customer.created_at,
                            customer.updated_at, customer.version
                        ))
                    
                    # Store accounts
                    for account in self.accounts:
                                            cursor.execute("""
                        INSERT INTO accounts (
                            id, customer_id, account_number, account_type, currency,
                            interest_rate, is_active, is_frozen, overdraft_protection, 
                            minimum_balance, monthly_fee, branch_code, routing_number, 
                            created_at, updated_at, version
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s
                        ) ON CONFLICT (id) DO NOTHING
                    """, (
                        account.id, account.customer_id, account.account_number,
                        account.account_type.value, account.currency, account.interest_rate,
                        account.is_active, account.is_frozen, account.overdraft_protection,
                        account.minimum_balance, account.monthly_fee, account.branch_code,
                        account.routing_number, account.created_at, account.updated_at,
                        account.version
                    ))
                    
                    conn.commit()
                    logger.info("Initial data stored in database")
        
        except Exception as e:
            logger.error("Failed to store initial data", error=str(e))
            raise
    
       
    def _select_customer_for_transaction(self) -> Customer:
        """Select customer based on behavioral patterns"""
        # Higher tier customers are more active
        weights = []
        for customer in self.customers:
            if customer.tier == CustomerTier.ENTERPRISE:
                weight = 4.0
            elif customer.tier == CustomerTier.VIP:
                weight = 3.0
            elif customer.tier == CustomerTier.PREMIUM:
                weight = 2.0
            else:
                weight = 1.0
            weights.append(weight)
        
        return random.choices(self.customers, weights=weights)[0]
    
    async def _generate_and_send_transaction(self):
        """Generate and send a single transaction with all related events"""
        try:
            # Update base time if needed for progressive time simulation
            await self._update_base_time_if_needed()
            
            # Select customer and account
            customer = self._select_customer_for_transaction()
            customer_accounts = self.customer_accounts.get(customer.id, [])
            
            if not customer_accounts:
                logger.warning("No accounts found for customer", customer_id=customer.id)
                return
            
            account = random.choice(customer_accounts)
            
            # Generate customer session first (occasionally)
            session = None
            if random.random() > 0.9:  # 10% chance
                # Use current progressive base time for session generation
                session = self.data_generator.generate_customer_session(customer)
                
                # Store customer session in database (will be streamed by Debezium)
                await self._store_customer_session(session)
            
            # Generate transaction with session time boundaries if available
            transaction = self.data_generator.generate_transaction(
                customer, 
                account,
                session_start=session.started_at if session else None,
                session_end=session.ended_at if session else None
            )
            
            # Store in database
            await self._store_transaction(transaction)
            
            self.stats['transactions_generated'] += 1
            
            if self.stats['transactions_generated'] % 100 == 0:
                logger.info("Transaction generation progress", 
                           count=self.stats['transactions_generated'])
        
        except Exception as e:
            logger.error("Failed to generate transaction", error=str(e))
            self.stats['errors'] += 1
    
    async def _store_transaction(self, transaction: Transaction):
        """Store transaction in database"""
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO transactions (
                            id, transaction_type, status, from_account_id, to_account_id,
                            customer_id, amount, currency, exchange_rate, fee_amount,
                            description, merchant_id, reference_number, authorization_code,
                            transaction_location, device_fingerprint, ip_address, user_agent,
                            risk_score, risk_level, compliance_flags, processing_time_ms,
                            network, card_last_four, tags, metadata, created_at, updated_at, version
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        transaction.id, transaction.transaction_type.value, transaction.status.value,
                        transaction.from_account_id, transaction.to_account_id, transaction.customer_id,
                        transaction.amount, transaction.currency, transaction.exchange_rate,
                        transaction.fee_amount, transaction.description, transaction.merchant_id,
                        transaction.reference_number, transaction.authorization_code,
                        json.dumps(transaction.transaction_location), transaction.device_fingerprint,
                        transaction.ip_address, transaction.user_agent, transaction.risk_score,
                        transaction.risk_level.value, json.dumps(transaction.compliance_flags),
                        transaction.processing_time_ms, transaction.network, transaction.card_last_four,
                        json.dumps(transaction.tags), json.dumps(transaction.metadata),
                        transaction.created_at, transaction.updated_at, transaction.version
                    ))
                    conn.commit()
        
        except Exception as e:
            logger.error("Failed to store transaction", error=str(e), transaction_id=transaction.id)
            raise
    
    async def _load_existing_merchants(self) -> List[Merchant]:
        """Load existing merchants from database"""
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, name, business_type, mcc_code, address, phone, website, tax_id,
                               is_active, created_at, updated_at, version
                        FROM merchants
                    """)
                    merchants: List[Merchant] = []
                    for row in cursor.fetchall():
                        try:
                            merchant = Merchant(
                                id=row[0],
                                name=row[1],
                                business_type=row[2],
                                mcc_code=row[3],
                                address=(json.loads(row[4]) if row[4] else None),
                                phone=row[5],
                                website=row[6],
                                tax_id=row[7],
                                is_active=row[8],
                            )
                            # Preserve timestamps/version if needed
                            merchant.created_at = row[9]
                            merchant.updated_at = row[10]
                            merchant.version = row[11] or 1
                            merchants.append(merchant)
                        except Exception as e:
                            logger.warning("Failed to parse merchant row", row=row, error=str(e))
                            continue
                    return merchants
        except Exception as e:
            logger.error("Failed to load existing merchants", error=str(e))
            raise


    

    
    async def _store_customer_session(self, session):
        """Store customer session in database"""
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO customer_sessions (
                            id, customer_id, session_id, channel, device_type,
                            ip_address, location, started_at, ended_at,
                            actions_count, transactions_count, created_at, updated_at, version
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        session.id, session.customer_id, session.session_id,
                        session.channel, session.device_type, session.ip_address,
                        json.dumps(session.location), session.started_at, session.ended_at,
                        session.actions_count, session.transactions_count,
                        session.created_at, session.updated_at, session.version
                    ))
                    conn.commit()
        
        except Exception as e:
            logger.error("Failed to store customer session", error=str(e), session_id=session.id)
            raise
    

    
    async def start(self):
        """Start the realtime data producer"""
        if self.is_running:
            logger.warning("Producer is already running")
            return
        
        logger.info("Starting realtime data producer", 
                   rate=self.production_rate,
                   customers=len(self.customers))
        
        self.is_running = True
        self.stats['start_time'] = datetime.now(timezone.utc)
        
        # Start background tasks
        tasks = [
            asyncio.create_task(self._transaction_generator_loop())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Producer tasks cancelled")
        except Exception as e:
            logger.error("Producer error", error=str(e))
            raise
        finally:
            self.is_running = False
    
    async def _transaction_generator_loop(self):
        """Main transaction generation loop"""
        while self.is_running:
            try:
                current_rate = self.production_rate * random.uniform(0.2, 2.0)
                interval = 1.0 / current_rate if current_rate > 0 else 1.0
                
                await self._generate_and_send_transaction()
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in transaction generator loop", error=str(e))
                await asyncio.sleep(1)  # Prevent tight error loop
    

    
    def stop(self):
        """Stop the producer gracefully"""
        logger.info("Stopping realtime data producer...")
        self.is_running = False
        
        # Close database connections
        
        logger.info("Producer stopped", 
                   total_transactions=self.stats['transactions_generated'],
                   total_events=self.stats['events_sent'])
    
    def reset_progressive_time(self):
        """Reset progressive time to initial value"""
        self.current_base_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        self.last_time_update = None
        self.data_generator.reset_base_time()
        
        # Save reset time to file
        self._save_progressive_time()
        
        logger.info("Progressive time reset to initial value and saved")
    
    def get_current_progressive_time(self) -> Optional[datetime]:
        """Get current progressive base time"""
        return self.current_base_time
    
    def advance_progressive_time(self, days: int = 1):
        """Manually advance progressive time by specified days and save to file"""
        self.current_base_time += timedelta(days=days)
        self.data_generator.base_time = self.current_base_time
        
        # Save to file
        self._save_progressive_time()
        
        logger.info("Progressive time manually advanced", 
                   days=days, 
                   new_time=self.current_base_time)
    
    def get_time_file_path(self) -> str:
        """Get the path to the progressive time file"""
        return os.path.abspath(self.time_file)


async def main():
    """Main entry point"""
    from config import config
    
    # Configure logging
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Create and start producer using config
    producer = RealtimeDataProducer(
        db_connection_string=config.DATABASE_URL,
        # kafka_bootstrap_servers no longer needed - using direct CDC
        production_rate=config.PRODUCTION_RATE,
        num_customers=config.NUM_CUSTOMERS
    )
    
    try:
        await producer.initialize()
        await producer.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error("Producer failed", error=str(e))
        sys.exit(1)
    finally:
        producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
