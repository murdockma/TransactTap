"""
MFA Handler Module

Functionality to handle various types of MFA
challenges during browser automation. Integrates with
OTP readers and other authentication methods
"""

import os
import time
from typing import Optional, Dict, Any, Callable

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from src.auth.otp_reader import OTPReader
from src.utils.logger import get_logger


class MFAHandler:
    """
    Class for handling Multi-Factor Authentication challenges
    
    Provides methods to handle various MFA challenges,
    including OTP codes, push notifications, and security questions
    """
    
    def __init__(self):
        """Initialize MFA handler"""
        self.logger = get_logger("mfa_handler")
        self.otp_reader = OTPReader()
    
    def handle_mfa(self, driver: webdriver.Chrome, bank_id: str) -> bool:
        """
        Detect and handle MFA challenges
        
        :param driver: Selenium WebDriver instance
        :param bank_id: Identifier for the bank
        :return: Boolean indicating success or failure
        """
        try:
            # Detect MFA type
            mfa_type = self._detect_mfa_type(driver, bank_id)
            
            if not mfa_type:
                self.logger.debug("No MFA challenge detected")
                return True
            
            self.logger.info(f"Detected MFA type: {mfa_type}")
            
            # Handle based on MFA type
            if mfa_type == "otp":
                return self._handle_otp(driver, bank_id)
            elif mfa_type == "push":
                return self._handle_push_notification(driver)
            elif mfa_type == "security_questions":
                return self._handle_security_questions(driver, bank_id)
            else:
                self.logger.warning(f"Unsupported MFA type: {mfa_type}")
                return self._handle_manually(driver)
            
        except Exception as e:
            self.logger.error(f"Error handling MFA: {str(e)}")
            return False
    
    def _detect_mfa_type(self, driver: webdriver.Chrome, bank_id: str) -> Optional[str]:
        """
        Detect type of MFA challenge
        
        :param driver: Selenium WebDriver instance
        :param bank_id: Identifier for the bank
        :return: String identifier for MFA type
        """
        # Check for OTP input field
        otp_indicators = [
            "input[name*='otp']",
            "input[id*='otp']",
            "input[placeholder*='code']",
            "input[aria-label*='verification']",
            "#otp",
            "//label[contains(text(), 'Verification Code')]",
            "//label[contains(text(), 'Security Code')]"
        ]
        
        for indicator in otp_indicators:
            by = By.XPATH if indicator.startswith("//") else By.CSS_SELECTOR
            if len(driver.find_elements(by, indicator)) > 0:
                return "security_questions"
        
        return None
    
    def _handle_otp(self, driver: webdriver.Chrome, bank_id: str) -> bool:
        """
        Handle OTP challenges
        
        :param driver: Selenium WebDriver instance
        :param bank_id: Identifier for the bank
        :return: Boolean indicating success or failure
        """
        try:
            self.logger.info(f"Handling OTP challenge for {bank_id}")
            
            # Find OTP input field
            otp_input = None
            otp_selectors = [
                "input[name*='otp']", "input[id*='otp']", "#otp",
                "input[placeholder*='code']", "input[aria-label*='verification']"
            ]
            
            for selector in otp_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    otp_input = elements[0]
                    break
            
            if not otp_input:
                self.logger.error("OTP input field not found")
                return False
            
            # Get OTP code
            otp_code = self.otp_reader.get_latest_code(
                provider=bank_id.replace("_", " ").title(),
                timeout=60
            )
            
            if not otp_code:
                # Prompt user if automated reading fails
                self.logger.info("Automated OTP reading failed, prompting user...")
                otp_code = input(f"Please enter the {bank_id} OTP code: ")
            
            # Enter OTP code
            otp_input.clear()
            otp_input.send_keys(otp_code)
            
            # Find and click submit button
            submit_buttons = driver.find_elements(
                By.XPATH, 
                "//button[contains(text(), 'Submit') or contains(text(), 'Continue') or contains(text(), 'Verify')]"
            )
            
            if not submit_buttons:
                submit_buttons = driver.find_elements(
                    By.CSS_SELECTOR, 
                    "button[type='submit'], input[type='submit']"
                )
            
            if submit_buttons:
                submit_buttons[0].click()
                time.sleep(5)
                
                # Check for error messages
                error_messages = driver.find_elements(
                    By.XPATH, 
                    "//div[contains(@class, 'error') or contains(@class, 'alert')]"
                )
                
                if error_messages and any(
                    "incorrect" in msg.text.lower() or 
                    "invalid" in msg.text.lower() or 
                    "failed" in msg.text.lower() 
                    for msg in error_messages
                ):
                    self.logger.error("OTP verification failed")
                    return False
                
                self.logger.info("OTP verification successful")
                return True
            else:
                self.logger.error("Submit button not found")
                return False
                
        except Exception as e:
            self.logger.error(f"Error handling OTP: {str(e)}")
            return False
    
    def _handle_push_notification(self, driver: webdriver.Chrome) -> bool:
        """
        Handle push notification authentication
        
        :param driver: Selenium WebDriver instance
        :return: Boolean indicating success or failure
        """
        try:
            self.logger.info("Handling push notification challenge")
            
            # Check if we need to initiate push
            push_buttons = driver.find_elements(
                By.XPATH, 
                "//button[contains(text(), 'Send Push')]"
            )
            
            if push_buttons:
                push_buttons[0].click()
            
            # Prompt user to approve push notification
            print("\n" + "="*50)
            print("PUSH NOTIFICATION SENT")
            print("Please approve the authentication request on your device")
            print("="*50 + "\n")
            
            # Wait for approval
            max_wait_time = 60 
            poll_interval = 2 
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                # Check for success
                success_indicators = [
                    "//div[contains(text(), 'successful')]",
                    "//div[contains(text(), 'verified')]",
                    "//h1[contains(text(), 'Welcome')]",
                    "//span[contains(text(), 'Account Summary')]"
                ]
                
                for indicator in success_indicators:
                    elements = driver.find_elements(By.XPATH, indicator)
                    if elements:
                        self.logger.info("Push notification approved")
                        return True
                
                # Check for failure
                failure_indicators = [
                    "//div[contains(text(), 'failed')]",
                    "//div[contains(text(), 'denied')]",
                    "//div[contains(text(), 'timed out')]"
                ]
                
                for indicator in failure_indicators:
                    elements = driver.find_elements(By.XPATH, indicator)
                    if elements:
                        self.logger.error("Push notification denied or timed out")
                        return False
                
                time.sleep(poll_interval)
            
            self.logger.error("Push notification verification timed out")
            return False
            
        except Exception as e:
            self.logger.error(f"Error handling push notification: {str(e)}")
            return False
    
    def _handle_security_questions(self, driver: webdriver.Chrome, bank_id: str) -> bool:
        """
        Handle security question challenges
        
        :param driver: Selenium WebDriver instance
        :param bank_id: Identifier for the bank
        :return: Boolean indicating success or failure
        """
        try:
            self.logger.info(f"Handling security questions for {bank_id}")
            
            # Get security question text
            question_elements = driver.find_elements(
                By.XPATH, 
                "//label[contains(text(), 'Security Question') or contains(text(), 'Question')]"
            )
            
            if not question_elements:
                self.logger.error("Security question not found")
                return False
            
            question_text = question_elements[0].text
            self.logger.info(f"Security question: {question_text}")
            
            # Load answers
            answers = self._load_security_answers(bank_id)
            
            if not answers:
                # Prompt user for answer if not available
                self.logger.info("No pre-configured answers, prompting user...")
                answer = input(f"Please enter answer for security question: {question_text}\n")
            else:
                # Try to find matching answer
                answer = None
                for q_pattern, ans in answers.items():
                    if q_pattern.lower() in question_text.lower():
                        answer = ans
                        break
                
                if not answer:
                    # If no match found, prompt user
                    self.logger.info("No matching answer found, prompting user...")
                    answer = input(f"Please enter answer for security question: {question_text}\n")
            
            # Find answer input field
            answer_input = driver.find_element(
                By.CSS_SELECTOR, 
                "input[name*='answer'], input[id*='securityAnswer']"
            )
            
            # Enter answer
            answer_input.clear()
            answer_input.send_keys(answer)
            
            # Find and click submit button
            submit_buttons = driver.find_elements(
                By.XPATH, 
                "//button[contains(text(), 'Submit') or contains(text(), 'Continue') or contains(text(), 'Next')]"
            )
            
            if not submit_buttons:
                submit_buttons = driver.find_elements(
                    By.CSS_SELECTOR, 
                    "button[type='submit'], input[type='submit']"
                )
            
            if submit_buttons:
                submit_buttons[0].click()
                time.sleep(3)
                
                # Check for error messages
                error_messages = driver.find_elements(
                    By.XPATH, 
                    "//div[contains(@class, 'error') or contains(@class, 'alert')]"
                )
                
                if error_messages and any(
                    "incorrect" in msg.text.lower() or 
                    "wrong" in msg.text.lower() 
                    for msg in error_messages
                ):
                    self.logger.error("Security question answer incorrect")
                    return False
                
                self.logger.info("Security question answered successfully")
                return True
            else:
                self.logger.error("Submit button not found")
                return False
                
        except Exception as e:
            self.logger.error(f"Error handling security questions: {str(e)}")
            return False
    
    def _load_security_answers(self, bank_id: str) -> Dict[str, str]:
        """
        Load security question answers from configuration
        
        :param bank_id: Identifier for the bank
        :return: Dictionary mapping question patterns to answers
        """
        return {
            "first pet": os.environ.get(f"{bank_id.upper()}_PET_NAME", ""),
            "mother maiden": os.environ.get(f"{bank_id.upper()}_MOTHER_MAIDEN", ""),
            "high school": os.environ.get(f"{bank_id.upper()}_HIGH_SCHOOL", ""),
            "first car": os.environ.get(f"{bank_id.upper()}_FIRST_CAR", ""),
            "birth city": os.environ.get(f"{bank_id.upper()}_BIRTH_CITY", "")
        }
    
    def _handle_manually(self, driver: webdriver.Chrome) -> bool:
        """
        Fallback method for manual MFA handling
        
        :param driver: Selenium WebDriver instance
        :return: Boolean indicating success or failure
        """
        self.logger.info("Falling back to manual MFA handling")
        
        try:
            # Take screenshot to show challenge
            screenshot_path = os.path.join(os.getcwd(), "mfa_screenshot.png")
            driver.save_screenshot(screenshot_path)
            
            # Prompt user to handle challenge
            self.logger.info(f"MFA screenshot saved to {screenshot_path}")
            input("Please handle the authentication challenge manually in the browser, then press Enter to continue...")
            
            time.sleep(5)
            
            # Check if we're still on authentication page
            auth_indicators = [
                "//div[contains(text(), 'authentication')]",
                "//div[contains(text(), 'verification')]",
                "//h1[contains(text(), 'Verify')]"
            ]
            
            for indicator in auth_indicators:
                elements = driver.find_elements(By.XPATH, indicator)
                if elements:
                    self.logger.warning("Still on authentication page, MFA may not be complete")
                    return False
            
            self.logger.info("Manual MFA handling appears successful")
            return True
                
        except Exception as e:
            self.logger.error(f"Error during manual MFA handling: {str(e)}")
            return False
