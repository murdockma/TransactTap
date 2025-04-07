"""
Base Extractor Module

Defines an abstract base class for all bank extractors.
"""

import abc
from datetime import datetime
from typing import List, Optional, Dict, Any
import logging

from src.models.transaction import Transaction
from src.utils.logger import get_logger
from src.auth.mfa_handler import MFAHandler


class BaseExtractor(abc.ABC):
    """
    Abstract base class for all bank data extractors
    
    Defines a common interface that all specific bank 
    extractors implement. Also provides some shared utility 
    functions for its subclasses
    """
    
    def __init__(self, bank_id: str, config: Dict[str, Any]):
        """
        Initialize base extractor
        
        :param bank_id: Unique identifier for bank
        :param config: Configuration dictionary for bank
        """
        self.bank_id = bank_id
        self.config = config
        self.logger = get_logger(f"extractor.{bank_id}")
        self.mfa_handler = MFAHandler()
        
        # Common config params
        self.base_url = config.get("base_url")
        self.username = config.get("username")
        self.password = config.get("password")
        
        # Validate
        if not self.base_url:
            raise ValueError(f"Missing base_url in configuration for {bank_id}")
        if not self.username or not self.password:
            raise ValueError(f"Missing credentials in configuration for {bank_id}")
    
    @abc.abstractmethod
    def login(self) -> bool:
        """
        Log in to the bank's website
        
        :return: Boolean indicating success or failure
        """
        pass
    
    @abc.abstractmethod
    def navigate_to_transactions(self, account_type: Optional[str] = None) -> bool:
        """
        Navigate to the transactions page for the account
        
        :param account_type: Optional account type (checking, savings, credit, etc.)
        :return: Boolean indicating success or failure
        """
        pass
    
    @abc.abstractmethod
    def download_transactions(self, start_date: datetime, end_date: datetime) -> List[Transaction]:
        """
        Download transactions for the date range
        
        :param start_date: Beginning date for transaction extraction
        :param end_date: End date for transaction extraction
        :return: List of transaction objects
        """
        pass
    
    @abc.abstractmethod
    def logout(self) -> bool:
        """
        Log out from the bank's website
        
        :return: Boolean indicating success or failure
        """
        pass
    
    def extract(self, start_date: datetime, end_date: datetime) -> List[Transaction]:
        """
        Extract transactions from the bank for the specified date range
        
        :param start_date: Beginning date for transaction extraction
        :param end_date: End date for transaction extraction
            
        :return: List of transaction objects
        """
        self.logger.info(f"Extracting transactions from {self.bank_id} between {start_date.date()} and {end_date.date()}")
        
        transactions = []
        
        try:
            # Login
            login_success = self.login()
            if not login_success:
                self.logger.error(f"Failed to login to {self.bank_id}")
                return transactions
            
            # Navigate to transactions page
            nav_success = self.navigate_to_transactions()
            if not nav_success:
                self.logger.error(f"Failed to navigate to transactions page for {self.bank_id}")
                self.logout()
                return transactions
            
            # Download transactions
            transactions = self.download_transactions(start_date, end_date)
            self.logger.info(f"Downloaded {len(transactions)} transactions from {self.bank_id}")
            
            # Add bank identifier
            for transaction in transactions:
                transaction.source = self.bank_id
            
        except Exception as e:
            self.logger.error(f"Error extracting data from {self.bank_id}: {str(e)}", exc_info=True)
        
        finally:
            # Attempt to logout
            try:
                self.logout()
            except Exception as e:
                self.logger.error(f"Error during logout from {self.bank_id}: {str(e)}", exc_info=True)
        
        return transactions
    
    def handle_unexpected_page(self, expected_element: str, timeout: int = 10) -> bool:
        """
        Handle cases where we end up on an unexpected page
        
        :param expected_element: CSS selector or XPath for an element we expect
        :param timeout: Timeout in seconds for waiting
        :return: Boolean indicating whether we were able to recover
        """
        self.logger.warning("Unexpected page encountered. Default handler invoked.")
        return False
    
    def get_account_types(self) -> List[str]:
        """
        Get available account types for bank
        
        :return: List of account type identifiers
        """
        # Default returns config-specified accounts
        return self.config.get("accounts", ["default"])
    