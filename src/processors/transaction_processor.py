"""
Transaction Processor Module

Functionality for processing and transforming raw transaction
data. Handles categorization, deduplication, and enrichment
"""

import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
import pandas as pd

from src.models.transaction import Transaction
from src.utils.config import ConfigManager
from src.utils.logger import get_logger


class TransactionProcessor:
    """
    Class for processing financial transaction data
    
    Creates methods to clean, deduplicate, categorize, and enrich
    transaction data from sources
    """
    
    def __init__(self):
        """Initialize the transaction processor"""
        self.logger = get_logger("transaction_processor")
        self.config_manager = ConfigManager()
        
        # Load category mappings
        self.category_mappings = self._load_category_mappings()
    
    def _load_category_mappings(self) -> Dict[str, Dict[str, str]]:
        """
        Load transaction category mappings from config
        :return: Dictionary mapping patterns to category information
        """
        try:
            mappings = self.config_manager.get_category_mappings()
            self.logger.info(f"Loaded {len(mappings)} category mappings")
            return mappings
        except Exception as e:
            self.logger.error(f"Error loading category mappings: {str(e)}")
            return {}
    
    def process(self, transactions: List[Transaction]) -> List[Transaction]:
        """
        Process list of transactions
        
        :param: transactions: List of transaction objects to process
        :return: Processed list of transaction objects
        """
        self.logger.info(f"Processing {len(transactions)} transactions")
        
        if not transactions:
            return []
        
        # Clean data
        cleaned_transactions = self._clean_transactions(transactions)
        self.logger.debug(f"Cleaned transactions: {len(cleaned_transactions)}")
        
        # Deduplicate
        deduplicated_transactions = self._deduplicate_transactions(cleaned_transactions)
        self.logger.debug(f"After deduplication: {len(deduplicated_transactions)} transactions")
        
        # Categorize
        categorized_transactions = self._categorize_transactions(deduplicated_transactions)
        self.logger.debug(f"Categorized transactions: {len(categorized_transactions)}")
        
        # Add flags and enrich
        enriched_transactions = self._enrich_transactions(categorized_transactions)
        self.logger.debug(f"Enriched transactions: {len(enriched_transactions)}")
        
        self.logger.info(f"Transaction processing complete. {len(enriched_transactions)} transactions processed.")
        return enriched_transactions
    
    def _clean_transactions(self, transactions: List[Transaction]) -> List[Transaction]:
        """
        Clean and standardize transaction data
        
        :param transactions: List of transaction objects
        :return: Cleaned list of transaction objects
        """
        cleaned = []
        
        for transaction in transactions:
            try:
                if transaction.amount == 0:
                    continue
                
                # Clean description
                if transaction.description:
                    transaction.description = ' '.join(transaction.description.split())
                    prefixes_to_remove = [
                        "DEBIT PURCHASE -", "CREDIT -", "ACH CREDIT -", 
                        "ACH DEBIT -", "POS PURCHASE -"
                    ]
                    for prefix in prefixes_to_remove:
                        if transaction.description.startswith(prefix):
                            transaction.description = transaction.description[len(prefix):].strip()
                
                if transaction.date:
                    transaction.date = datetime.combine(
                        transaction.date.date(), 
                        datetime.min.time()
                    )
                
                cleaned.append(transaction)
                
            except Exception as e:
                self.logger.error(f"Error cleaning transaction: {str(e)}")
        
        return cleaned
    
    def _deduplicate_transactions(self, transactions: List[Transaction]) -> List[Transaction]:
        """
        Remove duplicate transactions
        
        :param transactions: List of transaction objects
        :return: Deduplicated list of transaction objects
        """
        if not transactions:
            return []
        
        # Convert to DataFrame
        df_rows = [t.to_dict() for t in transactions]
        df = pd.DataFrame(df_rows)
        
        df['date_str'] = df['date'].apply(lambda x: x.split('T')[0] if isinstance(x, str) else x.strftime('%Y-%m-%d'))
        df['amount_rounded'] = df['amount'].round(2)
        
        # Deduplicate based on date, amount, and description
        df_dedup = df.drop_duplicates(subset=['date_str', 'amount_rounded', 'description'])
        
        # Filter out payment transfers between accounts when both sides are present
        # (e.g., credit card payments from checking account)
        transfer_pairs = []
        
        # Find potential transfer pairs (same date, opposite amounts)
        for account in df_dedup['account_type'].unique():
            df_account = df_dedup[df_dedup['account_type'] == account]
            
            for _, row in df_account.iterrows():
                potential_matches = df_dedup[
                    (df_dedup['date_str'] == row['date_str']) & 
                    (abs(df_dedup['amount_rounded'] + row['amount_rounded']) < 0.01) &
                    (df_dedup['account_type'] != row['account_type'])
                ]
                
                if len(potential_matches) > 0:
                    for _, match_row in potential_matches.iterrows():
                        if row['transaction_id'] < match_row['transaction_id']:
                            transfer_pairs.append((row['transaction_id'], match_row['transaction_id']))
                        else:
                            transfer_pairs.append((match_row['transaction_id'], row['transaction_id']))
        
        # Mark transfers
        for tx_id1, tx_id2 in transfer_pairs:
            df_dedup.loc[df_dedup['transaction_id'] == tx_id1, 'is_transfer'] = True
            df_dedup.loc[df_dedup['transaction_id'] == tx_id2, 'is_transfer'] = True
        
        # Convert back to transaction objects
        result = []
        for _, row in df_dedup.iterrows():
            row_dict = row.to_dict()
            row_dict.pop('date_str', None)
            row_dict.pop('amount_rounded', None)
          
            result.append(Transaction.from_dict(row_dict))
        
        return result
    
    def _categorize_transactions(self, transactions: List[Transaction]) -> List[Transaction]:
        """
        Categorize transactions using the loaded category mappings
        
        :param transactions: List of transaction objects
        :return: List of categorized transaction objects
        """
        if not self.category_mappings:
            self.logger.warning("No category mappings available, skipping categorization")
            return transactions
        
        for transaction in transactions:
            try:
                if transaction.category:
                    continue

                transaction.categorize(self.category_mappings)
                
            except Exception as e:
                self.logger.error(f"Error categorizing transaction: {str(e)}")
        
        return transactions
    
    def _enrich_transactions(self, transactions: List[Transaction]) -> List[Transaction]:
        """
        Enrich transactions with info and flags
        
        :param transactions: List of transaction objects
        :return: List of enriched transaction objects
        """
        # Detect recurring transactions
        for transaction in transactions:
            try:
                if not transaction.is_recurring:
                    transaction.detect_recurring(transactions)

                self._add_merchant_metadata(transaction)

                if transaction.category in ['Healthcare', 'Education', 'Charity']:
                    transaction.is_reimbursable = True
                
            except Exception as e:
                self.logger.error(f"Error enriching transaction: {str(e)}")
        
        return transactions
    
    def _add_merchant_metadata(self, transaction: Transaction) -> None:
        """
        Add merchant-specific metadata to transaction
        
        :param transaction: Transaction to enrich
        """
        desc_lower = transaction.description.lower()
        
        # Subscription services
        subscription_patterns = {
            'netflix': {'service_type': 'streaming', 'company': 'Netflix'},
            'spotify': {'service_type': 'streaming', 'company': 'Spotify'},
            'apple.com/bill': {'service_type': 'digital', 'company': 'Apple'},
            'amazon prime': {'service_type': 'shopping', 'company': 'Amazon'},
            'hulu': {'service_type': 'streaming', 'company': 'Hulu'},
            'disney+': {'service_type': 'streaming', 'company': 'Disney'},
        }
        
        for pattern, metadata in subscription_patterns.items():
            if pattern in desc_lower:
                transaction.metadata.update(metadata)
                transaction.is_recurring = True
                break
        
        # Food delivery
        food_delivery_patterns = {
            'doordash': 'DoorDash',
            'uber eats': 'Uber Eats',
            'grubhub': 'GrubHub',
            'postmates': 'Postmates',
        }
        
        for pattern, service in food_delivery_patterns.items():
            if pattern in desc_lower:
                transaction.metadata['delivery_service'] = service
                break
