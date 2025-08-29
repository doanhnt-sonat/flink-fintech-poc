"""
Realtime data producer for fintech streaming data
Continuously generates and stores realistic financial data in PostgreSQL using outbox pattern
"""

import asyncio
import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from decimal import Decimal
import uuid

import structlog

from models import (
    Customer, Account, Transaction, Merchant, 
    OutboxEvent, EventType, TransactionStatus,
    TransactionType, CustomerTier, RiskLevel
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
    - Pure Outbox Pattern: Data stored in PostgreSQL, streamed via Debezium
    """
    
    def __init__(self, 
                  db_connection_string: str,
                  production_rate: int = 10,  # transactions per second
                  num_customers: int = 100):
        
        self.db_manager = DatabaseManager(db_connection_string)
        # KafkaManager no longer needed - using outbox pattern with Debezium
        self.data_generator = AdvancedDataGenerator()
        self.production_rate = production_rate
        self.num_customers = num_customers
        
        # Runtime state
        self.is_running = False
        self.customers: List[Customer] = []
        self.accounts: List[Account] = []
        self.customer_accounts: Dict[str, List[Account]] = {}
        
        # Progressive time management
        # Load or initialize progressive time from file
        self.time_file = "progressive_time.json"
        self.current_base_time = self._load_progressive_time()
        self.last_time_update = None
        self.time_progression_interval = 3600  # Update base time every hour
        
        # Initialize data generator's base time immediately
        self.data_generator.base_time = self.current_base_time
        
        # Statistics
        self.stats = {
            'transactions_generated': 0,
            'events_sent': 0,
            'errors': 0,
            'start_time': None
        }
        
        # Kafka topics from config (for reference - created by Debezium from outbox table)
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
            
            # Advance base time by 1 hour
            if self.current_base_time is None:
                self.current_base_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
            else:
                self.current_base_time += timedelta(hours=1)
            
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
        
        # Initialize database tables
        self.db_manager.init_tables()
        
        # Generate initial customer and account data
        scenario_data = self.data_generator.generate_realistic_scenario(self.num_customers)
        
        self.customers = scenario_data['customers']
        self.accounts = scenario_data['accounts']
        
        # Build customer -> accounts mapping
        for account in self.accounts:
            if account.customer_id not in self.customer_accounts:
                self.customer_accounts[account.customer_id] = []
            self.customer_accounts[account.customer_id].append(account)
        
        # Store initial data in database
        await self._store_initial_data()
        
        logger.info("Producer initialized", 
                   customers=len(self.customers),
                   accounts=len(self.accounts))
    
    async def _store_initial_data(self):
        """Store initial customer and account data in database"""
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
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
    
    def _get_time_based_transaction_rate(self) -> float:
        """Get transaction rate based on time of day (realistic patterns)"""
        current_hour = datetime.now().hour
        
        # Business hours have higher transaction rates
        if 9 <= current_hour <= 17:  # Business hours
            return self.production_rate * random.uniform(1.5, 2.0)
        elif 18 <= current_hour <= 22:  # Evening peak
            return self.production_rate * random.uniform(1.2, 1.8)
        elif 6 <= current_hour <= 8:  # Morning
            return self.production_rate * random.uniform(0.8, 1.2)
        else:  # Night/early morning
            return self.production_rate * random.uniform(0.2, 0.5)
    
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
            
            # Generate transaction with current progressive base time
            transaction = self.data_generator.generate_transaction(customer, account)
            
            # Store in database
            await self._store_transaction(transaction)
            
            # Store events in outbox table for Debezium to stream
            await self._store_outbox_events(transaction, customer, account)
            
            # Generate related events
            await self._generate_related_events(transaction, customer, account)
            
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
    
    async def _store_outbox_event(self, outbox_event: OutboxEvent):
        """Store outbox event in database for Debezium to stream"""
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO outbox (
                            id, aggregate_type, aggregate_id, event_type, payload, 
                            created_at, version
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        outbox_event.id, outbox_event.aggregate_type, outbox_event.aggregate_id,
                        outbox_event.event_type, json.dumps(outbox_event.payload),
                        outbox_event.created_at, outbox_event.version
                    ))
                    conn.commit()
                    
        except Exception as e:
            logger.error("Failed to store outbox event", error=str(e), event_id=outbox_event.id)
            raise
    
    async def _store_outbox_events(self, transaction: Transaction, customer: Customer, account: Account):
        """Store events in outbox table for Debezium to automatically stream"""
        try:
            # Main transaction event
            transaction_event = OutboxEvent(
                aggregate_type='Transaction',
                aggregate_id=transaction.id,
                event_type='transaction_created',
                payload={
                    'event_id': str(uuid.uuid4()),
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'customer_id': customer.id,
                    'account_id': account.id,
                    'transaction': transaction.dict()
                }
            )
            
            # Transaction status event
            status_event = OutboxEvent(
                aggregate_type='Transaction',
                aggregate_id=transaction.id,
                event_type=EventType.TRANSACTION_INITIATED if transaction.status == TransactionStatus.PENDING 
                          else EventType.TRANSACTION_COMPLETED,
                payload=transaction.dict()
            )
            
            # Store both events in outbox table
            await self._store_outbox_event(transaction_event)
            await self._store_outbox_event(status_event)
            
            self.stats['events_sent'] += 2
            
            logger.info("Outbox events stored successfully", 
                       transaction_id=transaction.id,
                       events_count=2)
        
        except Exception as e:
            logger.error("Failed to store outbox events", error=str(e))
            raise
    
    async def _generate_related_events(self, transaction: Transaction, customer: Customer, account: Account):
        """Generate and send related events (fraud alerts, compliance, etc.)"""
        try:

            
            # Generate customer session data occasionally
            if random.random() > 0.9:  # 10% chance
                # Use current progressive base time for session generation
                session = self.data_generator.generate_customer_session(customer)
                
                # Store customer session event in outbox table
                session_outbox_event = OutboxEvent(
                    aggregate_type='CustomerSession',
                    aggregate_id=session.id,
                    event_type='customer_session',
                    payload={
                        'event_id': str(uuid.uuid4()),
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'customer_id': customer.id,
                        'session': session.dict()
                    }
                )
                
                await self._store_outbox_event(session_outbox_event)
                
                self.stats['events_sent'] += 1
        
        except Exception as e:
            logger.error("Failed to generate related events", error=str(e))
    
    # Method _send_to_kafka removed - now using outbox pattern with Debezium
    

    
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
            asyncio.create_task(self._transaction_generator_loop()),
            asyncio.create_task(self._stats_reporter_loop())
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
                current_rate = self._get_time_based_transaction_rate()
                interval = 1.0 / current_rate if current_rate > 0 else 1.0
                
                await self._generate_and_send_transaction()
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in transaction generator loop", error=str(e))
                await asyncio.sleep(1)  # Prevent tight error loop
    

    
    async def _stats_reporter_loop(self):
        """Statistics reporting loop"""
        while self.is_running:
            try:
                await asyncio.sleep(30)  # Report every 30 seconds
                
                if self.stats['start_time']:
                    runtime = datetime.now(timezone.utc) - self.stats['start_time']
                    rate = self.stats['transactions_generated'] / runtime.total_seconds()
                    
                    logger.info("Producer statistics",
                               transactions=self.stats['transactions_generated'],
                               events=self.stats['events_sent'],
                               errors=self.stats['errors'],
                               rate_per_second=round(rate, 2),
                               runtime_minutes=round(runtime.total_seconds() / 60, 2),
                               current_base_time=self.current_base_time,
                               time_progression_hours=round((self.current_base_time - datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)).total_seconds() / 3600, 1) if self.current_base_time else 0)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in stats reporter loop", error=str(e))
    
    def stop(self):
        """Stop the producer gracefully"""
        logger.info("Stopping realtime data producer...")
        self.is_running = False
        
        # Close database connections
        # KafkaManager no longer needed
        
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
        # kafka_bootstrap_servers no longer needed - using outbox pattern
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
