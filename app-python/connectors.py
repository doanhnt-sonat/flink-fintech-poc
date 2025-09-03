"""
PostgreSQL and Debezium connector setup for Python fintech app
Handles PostgreSQL connections and Debezium connector registration
"""

import json
import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from decimal import Decimal

import requests
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import structlog
from urllib.parse import urlparse, urlunparse



logger = structlog.get_logger()


class DatabaseManager:
    """Manages PostgreSQL database connections and operations"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        # Ensure target database exists before creating the engine/session
        self._ensure_database_exists()
        self.engine = create_engine(connection_string)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def get_connection(self):
        """Get raw psycopg2 connection"""
        return psycopg2.connect(self.connection_string)
    
    def get_session(self):
        """Get SQLAlchemy session"""
        return self.SessionLocal()
    
    def _build_admin_connection_string(self) -> str:
        """Build a connection string to the 'postgres' admin database on same host."""
        parsed = urlparse(self.connection_string)
        # Replace path with /postgres
        admin_path = '/postgres'
        admin_url = parsed._replace(path=admin_path)
        return urlunparse(admin_url)

    def _extract_database_name(self) -> str:
        parsed = urlparse(self.connection_string)
        # path like /fintech_demo
        return parsed.path.lstrip('/') or 'postgres'

    def _ensure_database_exists(self):
        """Create target database if it does not exist yet."""
        db_name = self._extract_database_name()
        try:
            # Try connecting to the target database
            with psycopg2.connect(self.connection_string) as _:
                return
        except psycopg2.OperationalError as e:
            # Likely database does not exist; attempt to create
            if 'does not exist' not in str(e):
                # Not a missing DB error; re-raise
                raise
            admin_conn_str = self._build_admin_connection_string()
            # Use explicit connection with autocommit to allow CREATE DATABASE
            conn = psycopg2.connect(admin_conn_str)
            try:
                # Ensure autocommit for CREATE DATABASE
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
                    exists = cur.fetchone() is not None
                    if not exists:
                        cur.execute(sql.SQL("CREATE DATABASE {}" ).format(sql.Identifier(db_name)))
            finally:
                conn.close()
        
    def init_tables(self):
        """Initialize database tables for the fintech app"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Customers table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS customers (
                        id VARCHAR(255) PRIMARY KEY,
                        first_name VARCHAR(255) NOT NULL,
                        last_name VARCHAR(255) NOT NULL,
                        email VARCHAR(255) UNIQUE NOT NULL,
                        phone VARCHAR(50),
                        date_of_birth DATE,
                        ssn VARCHAR(11),
                        address JSONB,
                        tier VARCHAR(50) DEFAULT 'basic',
                        risk_score DECIMAL(10,2) DEFAULT 0,
                        kyc_status VARCHAR(50) DEFAULT 'pending',
                        is_active BOOLEAN DEFAULT TRUE,
                        credit_score INTEGER,
                        annual_income DECIMAL(15,2),
                        employment_status VARCHAR(100),
                        onboarding_channel VARCHAR(50),
                        referral_code VARCHAR(50),
                        preferences JSONB DEFAULT '{}',
                        tags JSONB DEFAULT '[]',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        version INTEGER DEFAULT 1
                    )
                """)
                
                # Accounts table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS accounts (
                        id VARCHAR(255) PRIMARY KEY,
                        customer_id VARCHAR(255) REFERENCES customers(id),
                        account_number VARCHAR(255) UNIQUE NOT NULL,
                        account_type VARCHAR(50) NOT NULL,
                        currency VARCHAR(3) DEFAULT 'USD',
                        balance DECIMAL(15,2) DEFAULT 0,
                        available_balance DECIMAL(15,2) DEFAULT 0,
                        credit_limit DECIMAL(15,2),
                        interest_rate DECIMAL(5,4),
                        is_active BOOLEAN DEFAULT TRUE,
                        is_frozen BOOLEAN DEFAULT FALSE,
                        overdraft_protection BOOLEAN DEFAULT FALSE,
                        minimum_balance DECIMAL(15,2) DEFAULT 0,
                        monthly_fee DECIMAL(10,2) DEFAULT 0,
                        branch_code VARCHAR(20),
                        routing_number VARCHAR(20),
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        version INTEGER DEFAULT 1
                    )
                """)
                
                # Merchants table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS merchants (
                        id VARCHAR(255) PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        business_type VARCHAR(100),
                        mcc_code VARCHAR(10),
                        address JSONB,
                        phone VARCHAR(50),
                        website VARCHAR(255),
                        tax_id VARCHAR(50),
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        version INTEGER DEFAULT 1
                    )
                """)
                
                # Transactions table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS transactions (
                        id VARCHAR(255) PRIMARY KEY,
                        transaction_type VARCHAR(50) NOT NULL,
                        status VARCHAR(50) DEFAULT 'pending',
                        from_account_id VARCHAR(255),
                        to_account_id VARCHAR(255),
                        customer_id VARCHAR(255) REFERENCES customers(id),
                        amount DECIMAL(15,2) NOT NULL,
                        currency VARCHAR(3) DEFAULT 'USD',
                        exchange_rate DECIMAL(10,6),
                        fee_amount DECIMAL(10,2) DEFAULT 0,
                        description TEXT,
                        merchant_id VARCHAR(255),
                        reference_number VARCHAR(255) UNIQUE,
                        authorization_code VARCHAR(50),
                        transaction_location JSONB,
                        device_fingerprint VARCHAR(255),
                        ip_address INET,
                        user_agent TEXT,
                        risk_score DECIMAL(5,2) DEFAULT 0,
                        risk_level VARCHAR(20) DEFAULT 'low',
                        compliance_flags JSONB DEFAULT '[]',
                        processing_time_ms INTEGER,
                        network VARCHAR(50),
                        card_last_four VARCHAR(4),
                        tags JSONB DEFAULT '[]',
                        metadata JSONB DEFAULT '{}',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        version INTEGER DEFAULT 1
                    )
                """)
                

                

                
                # Customer sessions
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS customer_sessions (
                        id VARCHAR(255) PRIMARY KEY,
                        customer_id VARCHAR(255) REFERENCES customers(id),
                        session_id VARCHAR(255) NOT NULL,
                        channel VARCHAR(50),
                        device_type VARCHAR(50),
                        ip_address INET,
                        location JSONB,
                        started_at TIMESTAMP WITH TIME ZONE,
                        ended_at TIMESTAMP WITH TIME ZONE,
                        actions_count INTEGER DEFAULT 0,
                        transactions_count INTEGER DEFAULT 0,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        version INTEGER DEFAULT 1
                    )
                """)
                
                # Enable logical replication
                cursor.execute("ALTER TABLE customers REPLICA IDENTITY FULL")
                cursor.execute("ALTER TABLE accounts REPLICA IDENTITY FULL")
                cursor.execute("ALTER TABLE transactions REPLICA IDENTITY FULL")
                
                conn.commit()
                logger.info("Database tables initialized successfully")




class DebeziumConnectorManager:
    """Manages Debezium connector registration and configuration"""
    
    def __init__(self, connect_url: str):
        self.connect_url = connect_url
    
    def register_postgres_connector(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Register a PostgreSQL Debezium connector"""
        
        connector_config = {
            "name": config["connector_name"],
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "tasks.max": "1",
                "database.hostname": config.get("database_host", "postgres"),
                "database.port": str(config.get("database_port", 5432)),
                "database.user": config.get("database_user", "postgres"),
                "database.password": config.get("database_password", "postgres"),
                "database.dbname": config.get("database_name", "fintech_demo"),
                "topic.prefix": config.get("topic_prefix", "fintech"),
                "schema.include.list": config.get("schema_include", "public"),
                "table.include.list": config.get("table_include", "public.customers,public.accounts,public.transactions,public.merchants,public.customer_sessions"),
                "plugin.name": "pgoutput",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                # Ensure Debezium unwrap SMT enabled by default; allow override via config
                "transforms": config.get("transforms", "unwrap"),
                "transforms.unwrap.type": config.get("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState"),
                "transforms.unwrap.drop.tombstones": config.get("transforms.unwrap.drop.tombstones", "true"),
                "transforms.unwrap.delete.handling.mode": config.get("transforms.unwrap.delete.handling.mode", "rewrite"),
                "slot.name": config.get("slot_name", f"debezium_{config['connector_name']}"),
                "publication.name": config.get("publication_name", f"dbz_{config['connector_name']}")
            }
        }
        

        
        try:
            response = requests.post(
                f"{self.connect_url}/connectors",
                json=connector_config,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code in [200, 201]:
                logger.info("Connector registered successfully", 
                           connector=config["connector_name"])
                return response.json()
            # If already exists, update config idempotently via PUT
            if response.status_code == 409:
                update_resp = requests.put(
                    f"{self.connect_url}/connectors/{config['connector_name']}/config",
                    json=connector_config["config"],
                    headers={"Content-Type": "application/json"}
                )
                if update_resp.status_code in [200, 201]:
                    logger.info("Connector config updated (already existed)",
                                connector=config["connector_name"])
                    return update_resp.json() if update_resp.text else {"message": "updated"}
                logger.error("Failed to update existing connector",
                             status_code=update_resp.status_code,
                             response=update_resp.text)
                raise Exception(f"Connector update failed: {update_resp.text}")
            logger.error("Failed to register connector", 
                       status_code=response.status_code,
                       response=response.text)
            raise Exception(f"Connector registration failed: {response.text}")
                
        except requests.RequestException as e:
            logger.error("Error communicating with Kafka Connect", error=str(e))
            raise
    
    def delete_connector(self, connector_name: str):
        """Delete a connector"""
        try:
            response = requests.delete(f"{self.connect_url}/connectors/{connector_name}")
            
            if response.status_code == 204:
                logger.info("Connector deleted successfully", connector=connector_name)
            else:
                logger.error("Failed to delete connector", 
                           status_code=response.status_code,
                           response=response.text)
                
        except requests.RequestException as e:
            logger.error("Error communicating with Kafka Connect", error=str(e))
            raise
    
    def get_connector_status(self, connector_name: str) -> Dict[str, Any]:
        """Get connector status"""
        try:
            response = requests.get(f"{self.connect_url}/connectors/{connector_name}/status")
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error("Failed to get connector status", 
                           status_code=response.status_code)
                return {}
                
        except requests.RequestException as e:
            logger.error("Error communicating with Kafka Connect", error=str(e))
            return {}
    
    def list_connectors(self) -> List[str]:
        """List all connectors"""
        try:
            response = requests.get(f"{self.connect_url}/connectors")
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error("Failed to list connectors", 
                           status_code=response.status_code)
                return []
                
        except requests.RequestException as e:
            logger.error("Error communicating with Kafka Connect", error=str(e))
            return []


def setup_fintech_connectors():
    """Setup fintech Kafka connector"""
    from config import config
    
    connector_manager = DebeziumConnectorManager(connect_url=config.KAFKA_CONNECT_URL)
    
    # Get main connector config from config.py
    main_config = config.CONNECTOR_CONFIGS['main']
    
    try:
        # Register main connector only
        connector_manager.register_postgres_connector(main_config)
        
        logger.info("Fintech connector registered successfully")
        
        # Check status
        status = connector_manager.get_connector_status(main_config["connector_name"])
        logger.info("Connector status", 
                   connector=main_config["connector_name"],
                   status=status.get("connector", {}).get("state", "unknown"))
        
    except Exception as e:
        logger.error("Failed to setup connector", error=str(e))
        raise


if __name__ == "__main__":
    # Example usage
    from config import config
    import structlog
    
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
    
    # Initialize database using config
    db_manager = DatabaseManager(config.DATABASE_URL)
    db_manager.init_tables()
    
    # Setup connectors
    setup_fintech_connectors()
