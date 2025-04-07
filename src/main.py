"""
Main entry point

This script orchestrates the pipeline process:
1. Load configurations
2. Initialize bank extractors
3. Extract transaction data 
4. Process and transform data
5. Load data to BigQuery
6. Run data quality checks
"""

import os
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from src.utils.config import ConfigManager
from src.utils.logger import setup_logger
from src.extractors.extractor_factory import ExtractorFactory
from src.processors.transaction_processor import TransactionProcessor
from src.loaders.bigquery_loader import BigQueryLoader
from src.models.transaction import Transaction


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Personal Finance Data Pipeline")
    parser.add_argument(
        "--banks", 
        nargs="+", 
        default=["all"], 
        help="List of banks to process (default: all)"
    )
    parser.add_argument(
        "--start-date", 
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"), 
        help="Start date for transaction extraction (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date", 
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        default=datetime.now(),  
        help="End date for transaction extraction (YYYY-MM-DD, default: today)"
    )
    parser.add_argument(
        "--skip-extraction", 
        action="store_true", 
        help="Skip data extraction phase"
    )
    parser.add_argument(
        "--skip-loading", 
        action="store_true", 
        help="Skip data loading phase"
    )
    parser.add_argument(
        "--debug", 
        action="store_true", 
        help="Enable debug logging"
    )
    return parser.parse_args()


def extract_data(banks: List[str], start_date: datetime, end_date: datetime) -> Dict[str, List[Transaction]]:
    """
    Extract transaction data
    
    :param banks: List of bank identifiers to extract data from
    :param start_date Beginning date for transaction extraction
    :param end_date: End date for transaction extraction
    :return: Dictionary mapping bank names to lists of transaction objects
    """
    logger.info(f"Starting data extraction for {len(banks)} banks from {start_date.date()} to {end_date.date()}")
    
    extractor_factory = ExtractorFactory()
    all_transactions = {}
    
    # Extract data from each bank
    for bank in banks:
        try:
            logger.info(f"Extracting data from {bank}...")
            
            # Get appropriate extractor
            extractor = extractor_factory.get_extractor(bank)
            
            # Extract transactions
            transactions = extractor.extract(start_date, end_date)
            all_transactions[bank] = transactions
            
            logger.info(f"Successfully extracted {len(transactions)} transactions from {bank}")
            
        except Exception as e:
            logger.error(f"Error extracting data from {bank}: {str(e)}", exc_info=True)
    
    return all_transactions


def process_data(all_transactions: Dict[str, List[Transaction]]) -> List[Transaction]:
    """
    Process and transform extracted data
    
    :param all_transactions: Dictionary of transactions by bank
    :return: List of processed transaction objects
    """
    logger.info("Starting data processing...")
    
    # Flatten transactions
    flattened_transactions = []
    for bank, transactions in all_transactions.items():
        flattened_transactions.extend(transactions)
    
    # Process
    processor = TransactionProcessor()
    processed_transactions = processor.process(flattened_transactions)
    
    logger.info(f"Processed {len(processed_transactions)} transactions")
    return processed_transactions


def load_data(transactions: List[Transaction]) -> bool:
    """
    Load processed transaction data to BigQuery
    
    :param transactions: List of processed Transaction objects
    :return: Success status as boolean
    """
    logger.info("Starting data loading to BigQuery...")
    
    # Load transactions
    loader = BigQueryLoader()
    success = loader.load(transactions)
    
    if success:
        logger.info(f"Successfully loaded {len(transactions)} transactions to BigQuery")
    else:
        logger.error("Failed to load transactions to BigQuery")
    
    return success


def main():
    args = parse_arguments()
    
    global logger
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger = setup_logger("finance_pipeline", log_level)
    
    logger.info("Starting Personal Finance Data Pipeline")
    
    # Load config
    config_manager = ConfigManager()
    config = config_manager.load_config()
    
    # Determine which banks to process
    banks_to_process = config.get_bank_list() if "all" in args.banks else args.banks
    
    # Get start date from arguments or config
    start_date = args.start_date
    if start_date is None:
        if not args.skip_extraction:
            try:
                bq_loader = BigQueryLoader()
                start_date = bq_loader.get_latest_transaction_date()
                # Add one day to avoid dupes
                if start_date:
                    start_date = start_date + timedelta(days=1)
            except Exception as e:
                logger.warning(f"Could not fetch latest transaction date: {str(e)}")
                
        # If still none, use 30 days ago
        if start_date is None:
            start_date = datetime.now() - timedelta(days=30)
            logger.info(f"Using default start date: {start_date.date()}")
    
    # Extract data
    all_transactions = {}
    if not args.skip_extraction:
        all_transactions = extract_data(banks_to_process, start_date, args.end_date)
    else:
        logger.info("Skipping data extraction phase")
    
    # Process data
    processed_transactions = []
    if all_transactions:
        processed_transactions = process_data(all_transactions)
    
    # Load data
    if not args.skip_loading and processed_transactions:
        load_success = load_data(processed_transactions)
        if load_success:
            logger.info("Data pipeline completed successfully")
        else:
            logger.error("Data pipeline completed with errors in loading phase")
    elif args.skip_loading:
        logger.info("Skipping data loading phase")
        logger.info("Data pipeline completed successfully (loading skipped)")
    else:
        logger.warning("No transactions to load")
        logger.info("Data pipeline completed with warnings")


if __name__ == "__main__":
    main()
