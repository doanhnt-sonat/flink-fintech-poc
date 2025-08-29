"""
Main entry point for the fintech Python app
Provides CLI interface for running different components
"""

import asyncio
import sys
import argparse
import logging
from typing import Optional

import structlog

from data_generator import AdvancedDataGenerator
from connectors import DatabaseManager, setup_fintech_connectors
from realtime_producer import RealtimeDataProducer


def setup_logging(level: str = "INFO"):
    """Setup structured logging"""
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
    
    logging.basicConfig(level=getattr(logging, level.upper()))


async def init_database(db_connection: str):
    """Initialize database tables"""
    logger = structlog.get_logger()
    logger.info("Initializing database...")
    
    db_manager = DatabaseManager(db_connection)
    db_manager.init_tables()
    
    logger.info("Database initialized successfully")


async def setup_connectors():
    """Setup Debezium connectors"""
    logger = structlog.get_logger()
    logger.info("Setting up Debezium connectors...")
    
    setup_fintech_connectors()
    
    logger.info("Debezium connectors setup completed")


async def generate_sample_data(db_connection: str, num_customers: int = 10):
    """Generate and store sample data"""
    logger = structlog.get_logger()
    logger.info("Generating sample data...", num_customers=num_customers)
    
    generator = AdvancedDataGenerator()
    scenario_data = generator.generate_realistic_scenario(num_customers)
    
    # Store in database
    db_manager = DatabaseManager(db_connection)
    
    # This would need additional implementation to store all the generated data
    # For now, just log the statistics
    logger.info("Sample data generated",
                customers=len(scenario_data['customers']),
                accounts=len(scenario_data['accounts']),
                transactions=len(scenario_data['transactions']),
                fraud_alerts=len(scenario_data['fraud_alerts']))


async def start_producer(db_connection: str, 
                         rate: int = 5,
                         num_customers: int = 50):
    """Start the realtime data producer using outbox pattern"""
    logger = structlog.get_logger()
    logger.info("Starting realtime data producer using outbox pattern...", 
               rate=rate, 
               num_customers=num_customers)
    
    producer = RealtimeDataProducer(
        db_connection_string=db_connection,
        production_rate=rate,
        num_customers=num_customers
    )
    
    try:
        await producer.initialize()
        await producer.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, stopping producer...")
    except Exception as e:
        logger.error("Producer failed", error=str(e))
        raise
    finally:
        producer.stop()


def create_parser():
    """Create command line argument parser"""
    parser = argparse.ArgumentParser(description='Fintech Data Generator and Stream Producer')
    
    # Global options
    parser.add_argument('--db-connection', 
                       default='postgresql://postgres:postgres@localhost:5432/fintech_demo',
                       help='Database connection string')
    parser.add_argument('--log-level', 
                       default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Init database command
    init_parser = subparsers.add_parser('init-db', help='Initialize database tables')
    
    # Setup connectors command
    connector_parser = subparsers.add_parser('setup-connectors', help='Setup Debezium connectors')
    
    # Generate sample data command
    sample_parser = subparsers.add_parser('generate-sample', help='Generate sample data')
    sample_parser.add_argument('--num-customers', type=int, default=10,
                              help='Number of customers to generate')
    
    # Start producer command
    producer_parser = subparsers.add_parser('start-producer', help='Start realtime data producer')
    producer_parser.add_argument('--rate', type=int, default=5,
                                help='Transactions per second')
    producer_parser.add_argument('--num-customers', type=int, default=50,
                                help='Number of customers to simulate')
    
    # All-in-one command
    all_parser = subparsers.add_parser('run-all', help='Initialize everything and start producer')
    all_parser.add_argument('--rate', type=int, default=5,
                           help='Transactions per second')
    all_parser.add_argument('--num-customers', type=int, default=50,
                           help='Number of customers to simulate')
    all_parser.add_argument('--skip-connectors', action='store_true',
                           help='Skip Debezium connector setup')
    
    return parser


async def main():
    """Main entry point"""
    from config import config
    
    parser = create_parser()
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = structlog.get_logger()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'init-db':
            await init_database(args.db_connection or config.DATABASE_URL)
        
        elif args.command == 'setup-connectors':
            await setup_connectors()
        
        elif args.command == 'generate-sample':
            await generate_sample_data(args.db_connection or config.DATABASE_URL, args.num_customers or config.NUM_CUSTOMERS)
        
        elif args.command == 'start-producer':
            await start_producer(args.db_connection or config.DATABASE_URL, 
                                args.rate or config.PRODUCTION_RATE, 
                                args.num_customers or config.NUM_CUSTOMERS)
        
        elif args.command == 'run-all':
            logger.info("Running full setup and producer using outbox pattern...")
            
            # Initialize database
            await init_database(args.db_connection or config.DATABASE_URL)
            
            # Setup connectors (unless skipped)
            if not args.skip_connectors:
                await setup_connectors()
                # Wait a bit for connectors to be ready
                await asyncio.sleep(5)
            
            # Start producer (data will be streamed via Debezium outbox pattern)
            await start_producer(args.db_connection or config.DATABASE_URL,
                                args.rate or config.PRODUCTION_RATE,
                                args.num_customers or config.NUM_CUSTOMERS)
        
        else:
            parser.print_help()
    
    except Exception as e:
        logger.error("Command failed", command=args.command, error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
