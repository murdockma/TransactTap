"""
Chase Bank Extractor

Uses SeleniumExtractor specifically for Chase Bank.
Handle s navigation patterns, authentication flows, and data
extraction methods specific to Chase's online banking
"""

import os
import time
import re
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from src.extractors.selenium_extractor import SeleniumExtractor
from src.models.transaction import Transaction
from src.auth.otp_reader import OTPReader


class ChaseExtractor(SeleniumExtractor):
    """
    Chase specific implementation of the Selenium extractor
    
    Handles specific login flows, navigation patterns,
    and transaction downloads for Chase's online banking
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Chase extractor
        
        :param config: Configuration dictionary for Chase
        """
        super().__init__("chase", config)
        self.otp_reader = OTPReader()
        self.account_map = {
            "checking": "CHECKING",
            "savings": "SAVINGS", 
            "credit": "CREDIT_CARD"
        }
    
    def login(self) -> bool:
        """
        Log in to Chase online banking
        
        :return: Boolean indicating success or failure
        """
        try:
            self.logger.info("Logging in to Chase...")
            
            # Navigate to Chase homepage
            self.driver.get(self.base_url)
            
            # Wait for username field
            username_field = self.find_element('#userId-input-field', timeout=10)
            password_field = self.find_element('#password-input-field', timeout=5)
            
            if not username_field or not password_field:
                self.logger.error("Login form not found")
                return False
            
            # Enter creds
            self.type_text('#userId-input-field', self.username)
            self.type_text('#password-input-field', self.password)
            
            # Find and click sign-in button
            signin_button = self.find_element('button[type="submit"]')
            if not signin_button:
                self.logger.error("Sign-in button not found")
                return False
            
            # Click sign in button
            if not self.click_element(signin_button):
                self.logger.error("Failed to click sign-in button")
                return False
            
            time.sleep(3)
            
            # Check for "Remember this device" prompt
            remember_device = self.find_element('//label[contains(text(), "Remember this device")]', by=By.XPATH, timeout=5)
            if remember_device:
                # Find checkbox and click it
                checkbox = self.find_element('#rememberComputer')
                if checkbox:
                    self.click_element(checkbox)
                
                # Click continue button
                continue_button = self.find_element('button[data-testid="requestIdentificationCode"]')
                if continue_button:
                    self.click_element(continue_button)
            
            # Check if we need to handle MFA
            if self.is_otp_required():
                self.logger.info("OTP verification required")
                return self.handle_otp_verification()
            
            # Check if login was successful
            return self.is_login_successful()
            
        except Exception as e:
            self.logger.error(f"Error during login: {str(e)}", exc_info=True)
            return False
    
    def is_otp_required(self) -> bool:
        """
        Check if OTP verification is required
        
        :return: Boolean indicating whether OTP is required
        """
        time.sleep(3)
        
        # Check for OTP indicators
        otp_indicators = [
            '#otpcode_input-input-field',
            '//h3[contains(text(), "Enter your code")]',
            '//p[contains(text(), "sent you a code")]'
        ]
        
        for indicator in otp_indicators:
            by = By.XPATH if indicator.startswith('//') else By.CSS_SELECTOR
            if self.find_element(indicator, by=by, timeout=5):
                return True
        
        return False
    
    def handle_otp_verification(self) -> bool:
        """
        Handle the OTP verification process
        
        :return: Boolean indicating success or failure
        """
        try:
            # Wait for OTP input field
            otp_field = self.find_element('#otpcode_input-input-field', timeout=10)
            if not otp_field:
                self.logger.error("OTP input field not found")
                return False
            
            # Get OTP using OTP reader
            self.logger.info("Waiting for OTP message...")
            otp_code = self.otp_reader.get_latest_code(
                provider="Chase",
                timeout=60,
                regex=r"security code: (\d{6})"
            )
            
            if not otp_code:
                # Prompt user if automated reading fails
                self.logger.info("Automated OTP reading failed, prompting user...")
                otp_code = input("Please enter the Chase OTP value: ")
            
            # Enter OTP
            self.type_text('#otpcode_input-input-field', otp_code)
            
            # Click Submit button
            submit_button = self.find_element('button[data-testid="requestIdentificationCodeSubmit"]')
            if not submit_button:
                self.logger.error("Submit button not found")
                return False
            
            self.click_element(submit_button)

            time.sleep(5)
            
            return self.is_login_successful()
            
        except Exception as e:
            self.logger.error(f"Error during OTP verification: {str(e)}", exc_info=True)
            return False
    
    def is_login_successful(self) -> bool:
        """
        Check if login was successful by looking for dashboard elements
        
        :return: Boolean indicating whether login was successful
        """
        # Look for successful login
        success_indicators = [
            '//span[contains(text(), "Accounts")]',
            '//h2[contains(text(), "Hello")]',
            '.account-tile'
        ]
        
        for indicator in success_indicators:
            by = By.XPATH if indicator.startswith('//') else By.CSS_SELECTOR
            if self.find_element(indicator, by=by, timeout=10):
                self.logger.info("Login successful")
                return True
        
        self.logger.error("Login unsuccessful - could not find dashboard elements")
        return False
    
    def navigate_to_transactions(self, account_type: Optional[str] = "checking") -> bool:
        """
        Navigate to the transactions download page
        
        :param account_type: Account type (checking, savings, credit)
        :return: Boolean indicating success or failure
        """
        try:
            self.logger.info(f"Navigating to {account_type} transactions page...")
            
            # Wait for accounts
            self.find_element('.account-tile', timeout=10)
            
            # Click on appropriate account based on account_type
            account_name = self.account_map.get(account_type.lower(), "CHECKING")
            account_xpath = f'//div[contains(@class, "account-tile") and contains(., "{account_name}")]'
            
            account_tile = self.find_element(account_xpath, by=By.XPATH)
            if not account_tile:
                self.logger.error(f"Account tile for {account_type} not found")
                return False
            
            self.click_element(account_tile, by=By.XPATH)

            time.sleep(5)
            
            # Click on Activity & Statements tab/link
            activity_tab = self.find_element('//a[contains(text(), "See activity")]', by=By.XPATH)
            if not activity_tab:
                activity_tab = self.find_element('//a[contains(text(), "Activity & statements")]', by=By.XPATH)
            
            if not activity_tab:
                self.logger.error("Activity tab not found")
                return False
            
            self.click_element(activity_tab, by=By.XPATH)
            
            # Wait for activity page to load
            time.sleep(3)
            
            # Look for download link/button
            download_button = self.find_element('//a[contains(text(), "Download")]', by=By.XPATH)
            if not download_button:
                self.logger.error("Download button not found")
                return False
            
            self.click_element(download_button, by=By.XPATH)

            time.sleep(2)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error navigating to transactions page: {str(e)}", exc_info=True)
            return False
    
    def download_transactions(self, start_date: datetime, end_date: datetime) -> List[Transaction]:
        """
        Download transactions for the date range
        
        :param start_date: Beginning date for transaction extraction
        :param end_date: End date for transaction extraction
        :return: List of Transaction objects
        """
        try:
            self.logger.info(f"Downloading transactions from {start_date.date()} to {end_date.date()}...")

            time.sleep(2)
            
            # Select CSV format
            format_select = self.find_element('#download-type-select')
            if not format_select:
                self.logger.error("Format selection dropdown not found")
                return []
            
            self.click_element(format_select)
            
            # Select CSV option
            csv_option = self.find_element('//option[contains(text(), "CSV")]', by=By.XPATH)
            if not csv_option:
                self.logger.error("CSV option not found")
                return []
            
            self.click_element(csv_option, by=By.XPATH)
            
            # Set date range
            date_range_select = self.find_element('#date-range-select')
            if date_range_select:
                self.click_element(date_range_select)
                
                # Select custom date range
                custom_option = self.find_element('//option[contains(text(), "Custom date")]', by=By.XPATH)
                if custom_option:
                    self.click_element(custom_option, by=By.XPATH)
            
            # Set start date
            start_date_input = self.find_element('#start-date-input-field')
            if start_date_input:
                self.type_text('#start-date-input-field', start_date.strftime('%m/%d/%Y'), clear_first=True)
            
            # Set end date
            end_date_input = self.find_element('#end-date-input-field')
            if end_date_input:
                self.type_text('#end-date-input-field', end_date.strftime('%m/%d/%Y'), clear_first=True)
            
            # Click download button
            download_button = self.find_element('button[data-testid="download-button"]')
            if not download_button:
                self.logger.error("Download button not found")
                return []
            
            self.click_element(download_button)
            
            # Wait for download to complete
            downloaded_file = self.wait_for_download(timeout=60)
            if not downloaded_file:
                self.logger.error("Download failed or timed out")
                return []
            
            # Process downloaded file
            return self._process_downloaded_file(downloaded_file, account_type="checking")
            
        except Exception as e:
            self.logger.error(f"Error downloading transactions: {str(e)}", exc_info=True)
            return []
    
    def _process_downloaded_file(self, file_path: str, account_type: str) -> List[Transaction]:
        """
        Process the downloaded CSV file into transaction objects
        
        :param file_path: Path to the downloaded file
        :param account_type: Type of account (checking, savings, credit)
        :return: List of rransaction objects
        """
        try:
            self.logger.info(f"Processing downloaded file: {file_path}")
            
            # Chase CSV files typically have different column layouts depending on account type
            if account_type == "credit":
                # Credit card transactions
                column_names = ['transaction_date', 'post_date', 'description', 'category', 'transaction_type', 'amount']
            else:
                # Checking/savings transactions
                column_names = ['transaction_date', 'post_date', 'description', 'amount', 'transaction_type', 'balance']
            
            # Read CSV file
            df = pd.read_csv(file_path, header=0)
            
            # For credit cards, amounts need sign adjustment
            if account_type == "credit":
                # Negative amounts for purchases (debits), positive for credits
                df['amount'] = df['amount'].apply(lambda x: -x if "DEBIT" in str(df['transaction_type']).upper() else x)
            
            # Convert data types
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
            
            # Drop rows with invalid dates or amounts
            df = df.dropna(subset=['transaction_date', 'amount'])
            
            # Create transaction objects
            transactions = []
            for _, row in df.iterrows():
                transaction = Transaction(
                    date=row['transaction_date'],
                    amount=row['amount'],
                    description=row['description'],
                    account_type=account_type,
                    source='chase'
                )
                transactions.append(transaction)
            
            self.logger.info(f"Processed {len(transactions)} transactions")
            return transactions
            
        except Exception as e:
            self.logger.error(f"Error processing downloaded file: {str(e)}", exc_info=True)
            return []
        