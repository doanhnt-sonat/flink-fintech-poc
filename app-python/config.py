"""
Configuration settings for the fintech Python app
"""

import os


class Config:
    """Configuration class with environment variable support"""
    
    # Database settings
    DATABASE_HOST = os.getenv('DATABASE_HOST', 'postgres')
    DATABASE_PORT = int(os.getenv('DATABASE_PORT', '5432'))
    DATABASE_NAME = os.getenv('DATABASE_NAME', 'fintech_demo')
    DATABASE_USER = os.getenv('DATABASE_USER', 'postgres')
    DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD', 'postgres')
    
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.DATABASE_USER}:{self.DATABASE_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}"
    
    # Kafka Connect settings (for Debezium connector registration)
    KAFKA_CONNECT_URL = os.getenv('KAFKA_CONNECT_URL', 'http://kafka-connect:8083')
    
    # Producer settings
    PRODUCTION_RATE = int(os.getenv('PRODUCTION_RATE', '5'))  # transactions per second
    NUM_CUSTOMERS = int(os.getenv('NUM_CUSTOMERS', '50'))
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Topics configuration
    # Kafka topics (for reference - created by Debezium from outbox table)
    KAFKA_TOPICS = {
        'transactions': 'fintech.transactions',
        'customers': 'fintech.customers', 
        'accounts': 'fintech.accounts',
        'fraud_alerts': 'fintech.fraud_alerts',
        'events': 'fintech.events',
        'market_data': 'fintech.market_data',
        'sessions': 'fintech.sessions'
    }
    
    # Connector configurations
    CONNECTOR_CONFIGS = {
        'main': {
            'connector_name': 'fintech-main-connector',
            'database_host': DATABASE_HOST,
            'database_port': DATABASE_PORT,
            'database_user': DATABASE_USER,
            'database_password': DATABASE_PASSWORD,
            'database_name': DATABASE_NAME,
            'topic_prefix': 'fintech',
            'table_include': 'public.customers,public.accounts,public.transactions,public.merchants,public.fraud_alerts',
            'slot_name': 'debezium_main',
            'publication_name': 'dbz_main'
        },
        'outbox': {
            'connector_name': 'fintech-outbox-connector',
            'database_host': DATABASE_HOST,
            'database_port': DATABASE_PORT,
            'database_user': DATABASE_USER,
            'database_password': DATABASE_PASSWORD,
            'database_name': DATABASE_NAME,
            'topic_prefix': 'fintech.events',
            'table_include': 'public.outbox',
            'enable_outbox': True,
            'slot_name': 'debezium_outbox',
            'publication_name': 'dbz_outbox'
        },
        'analytics': {
            'connector_name': 'fintech-analytics-connector',
            'database_host': DATABASE_HOST,
            'database_port': DATABASE_PORT,
            'database_user': DATABASE_USER,
            'database_password': DATABASE_PASSWORD,
            'database_name': DATABASE_NAME,
            'topic_prefix': 'fintech.analytics',
            'table_include': 'public.account_balances,public.customer_sessions',
            'slot_name': 'debezium_analytics',
            'publication_name': 'dbz_analytics'
        }
    }


# Global config instance
config = Config()
