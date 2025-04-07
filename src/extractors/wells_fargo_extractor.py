"""
Wells Fargo Bank Extractor

Implements SeleniumExtractor specifically for Wells Fargo Bank.
Handles navigation patterns, authentication flows, and data
extraction specific to Wells Fargo's online banking.
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


class WellsFargoExtractor(SeleniumExtractor):
    """
    Wells Fargo implementation of the Selenium extractor
    
    Implements the specific login flows, navigation patterns,
    and transaction downloads for Wells Fargo online banking
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Wells Fargo extractor
        
        :param config: Configuration dictionary for Wells Fargo
        """
        super().__init__("wells_fargo", config)
        self.otp_reader = OTPReader()
        self.account_map = {
            "checking": "DDA",
            "savings": "SDA", 
            "credit": "CCA"
        }
    
    def login(self) -> bool:
        """
        Log in to Wells Fargo online banking
        
        :return: Boolean indicating success or failure
        """
        try:
            self.logger.info("Logging in to Wells Fargo...")
            
            # Navigate to homepage
            self.driver.get(self.base_url)
            
            # Look for the sign-on link - diferent approaches becuz the site sometimes changes
            signin_selectors = [
                'div.ps-masthead-sign-on a.ps-sign-on-text',
                'a.signIn',
                '[data-testid="sign-on"]',
                '//a[contains(text(), "Sign On")]'
            ]
            
            signin_clicked = False
            for selector in signin_selectors:
                by = By.XPATH if selector.startswith('//') else By.CSS_SELECTOR
                signin_element = self.find_element(selector, by=by, timeout=5)
                if signin_element:
                    signin_clicked = self.click_element(signin_element, by=by)
                    if signin_clicked:
                        break
            
            if not signin_clicked:
                self.logger.error("Could not find sign-on link")
                return False
            
            # Wait for username and password fields
            username_field = self.find_element('#j_username', timeout=10)
            password_field = self.find_element('#j_password', timeout=5)
            
            if not username_field or not password_field:
                self.logger.error("Login form not found")
                return False
            
            # Enter creds
            self.type_text('#j_username', self.username)
            self.type_text('#j_password', self.password)
            
            # Find and click sign-on button
            signin_button = self.find_element('[data-testid="signon-button"]')
            if not signin_button:
                self.logger.error("Sign-on button not found")
                return False
            
            # Click sign on button with retry
            signin_success = False
            for _ in range(2):  # Try up to 2 times
                if self.click_element(signin_button):
                    signin_success = True
                    break
                time.sleep(1)
            
            if not signin_success:
                self.logger.error("Failed to click sign-on button")
                return False
            
            # Handle OTP verification if needed
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
        # Wait for OTP page to load
        time.sleep(3)
        
        # Check for OTP indicators
        otp_indicators = [
            'li.LineItemLinkList__lineItemLinkListItem___HHmyb button.Button__button___Jo8E3',
            '#otp',
            '//h1[contains(text(), "Verification Code")]',
            '//div[contains(text(), "verification code")]'
        ]
        
        for indicator in otp_indicators:
            by = By.XPATH if indicator.startswith('//') else By.CSS_SELECTOR
            if self.find_element(indicator, by=by, timeout=5):
                return True
        
        return False
    
    def handle_otp_verification(self) -> bool:
        """
        Handle OTP verification process
        
        :return: Boolean indicating success or failure
        """
        try:
            # Try to click the button to request an OTP
            otp_request_button = self.find_element('li.LineItemLinkList__lineItemLinkListItem___HHmyb button.Button__button___Jo8E3')
            if otp_request_button:
                self.click_element(otp_request_button)
            
            # Wait for OTP input field
            otp_field = self.find_element('#otp', timeout=10)
            if not otp_field:
                self.logger.error("OTP input field not found")
                return False
            
            # Get OTP using the OTP reader
            self.logger.info("Waiting for OTP message...")
            otp_code = self.otp_reader.get_latest_code(
                provider="Wells Fargo",
                timeout=60,
                regex=r"verification code: (\d{6})"
            )
            
            if not otp_code:
                # If automated reading fails, prompt user
                self.logger.info("Automated OTP reading failed, prompting user...")
                otp_code = input("Please enter the Wells Fargo OTP value: ")
            
            # Enter OTP
            self.type_text('#otp', otp_code)
            
            # Click Continue button
            continue_button = self.find_element('//button[span[text()="Continue"]]', by=By.XPATH)
            if not continue_button:
                self.logger.error("Continue button not found")
                return False
            
            self.click_element(continue_button, by=By.XPATH)
            
            # Wait for dashboard to load
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
        # Look for indicators of successful login
        success_indicators = [
            '//*[@id="S_ACCOUNTS"]/div/div/span',
            '//span[contains(text(), "Account Summary")]',
            '[data-testid="account-group-DDA"]' 
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
            
            # Navigate to accounts section if not already there
            accounts_element = self.find_element('//*[@id="S_ACCOUNTS"]/div/div/span', by=By.XPATH)
            if accounts_element:
                self.click_element(accounts_element, by=By.XPATH)
            
            # Wait for page to load
            time.sleep(3)
            
            # Look for "Download Account Activity" link
            download_link = self.find_element('//*[text()="Download Account Activity"]', by=By.XPATH)
            if not download_link:
                self.logger.error("Download Account Activity link not found")
                return False
            
            self.click_element(download_link, by=By.XPATH)
            
            # Wait for download page to load
            self.find_element('#fromDate', timeout=10)
            
            # Check if need to select an account
            account_dropdown = self.find_element('#downloadAccountSelect')
            if account_dropdown:
                self.logger.debug("Account selection dropdown found, selecting account...")
                
                # Get proper account code
                account_code = self.account_map.get(account_type.lower(), "DDA")
                
                # Find account option with code
                account_option = self.find_element(f'option[value*="{account_code}"]')
                if account_option:
                    self.click_element(account_option)
                    time.sleep(2)  # Wait for selection
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error navigating to transactions page: {str(e)}", exc_info=True)
            return False
    
    def download_transactions(self, start_date: datetime, end_date: datetime) -> List[Transaction]:
        """
        Download transactions for date range
        
        :param start_date: Beginning date for transaction extraction
        :param end_date: End date for transaction extraction
        :return: List of transaction objects
        """
        try:
            self.logger.info(f"Downloading transactions from {start_date.date()} to {end_date.date()}...")
            
            # Format dates for input fields
            start_date_str = start_date.strftime('%m/%d/%Y')
            end_date_str = end_date.strftime('%m/%d/%Y')
            
            # Clear and fill date fields
            from_date_field = self.find_element('#fromDate')
            to_date_field = self.find_element('#toDate')
            
            if not from_date_field or not to_date_field:
                self.logger.error("Date fields not found")
                return []
            
            # Clear and enter start and end date
            self.driver.execute_script('document.querySelector("#fromDate").value = ""')
            self.type_text('#fromDate', start_date_str)
            self.driver.execute_script('document.querySelector("#toDate").value = ""')
            self.type_text('#toDate', end_date_str)
            
            # Select CSV format
            csv_radio = self.find_element('[data-testid="radio-fileFormat-commaDelimited"]')
            if not csv_radio:
                self.logger.error("CSV format option not found")
                return []
            
            self.click_element(csv_radio)
            
            # Click download button
            download_button = self.find_element('[data-testid="download-button"]')
            if not download_button:
                self.logger.error("Download button not found")
                return []
            
            self.click_element(download_button)
            
            # Handle confirmation dialogs
            try:
                # Wait a bit for dialog
                time.sleep(3)
                
                # Handle dialog (may be needed multiple times)
                for _ in range(3):
                    self.driver.switch_to.active_element.send_keys('\n')
                    time.sleep(1)
                
            except Exception as e:
                self.logger.debug(f"Exception while handling confirmation: {str(e)}")
            
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
        Process the downloaded CSV file into Transaction objects
        
        :param file_path: Path to the downloaded file
        :param account_type: Type of account (checking, savings, credit)
        :return: List of transaction objects
        """
        try:
            self.logger.info(f"Processing downloaded file: {file_path}")
            
            column_names = ['date', 'amount', 'unused1', 'unused2', 'description']
            
            # Read CSV file
            df = pd.read_csv(file_path, header=None, names=column_names)
            
            # Filter out header rows and irrelevant transactions
            df = df[~df['description'].str.contains('ONLINE PAYMENT THANK YOU|AUTOMATIC PAYMENT - THANK YOU', na=False)]
            
            # Convert data types
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
            
            # Drop rows with invalid dates / amounts
            df = df.dropna(subset=['date', 'amount'])
            
            # Create transaction objects
            transactions = []
            for _, row in df.iterrows():
                transaction = Transaction(
                    date=row['date'],
                    amount=row['amount'],
                    description=row['description'],
                    account_type=account_type,
                    source='wells_fargo'
                )
                transactions.append(transaction)
            
            self.logger.info(f"Processed {len(transactions)} transactions")
            return transactions
            
        except Exception as e:
            self.logger.error(f"Error processing downloaded file: {str(e)}", exc_info=True)
            return []
        