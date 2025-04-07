"""
CAPTCHA Solver Module

Functionality to handle CAPTCHA challenges during 
browser automation. Includes manual method for
integration with solving services
"""

import os
import time
import base64
from typing import Optional, Dict, Any, Union

import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from src.utils.logger import get_logger


class CaptchaSolver:
    """
    Class for solving CAPTCHA challenges during browser automation
    
    Provides ways to detect and solve types of CAPTCHA
    challenges, like image-based CAPTCHAs, audio CAPTCHAs, and reCAPTCHA
    """
    
    def __init__(self, service_key: Optional[str] = None):
        """
        Initialize CAPTCHA solver
        
        :param service_key: Optional API key for external CAPTCHA solving service
        """
        self.logger = get_logger("captcha_solver")
        self.service_key = service_key or os.environ.get("CAPTCHA_SERVICE_KEY")
    
    def solve_captcha(self, driver: webdriver.Chrome) -> bool:
        """
        Detect and solve CAPTCHA challenge
        
        :param driver: Selenium WebDriver instance
        :return: Boolean indicating success or failure
        """
        try:
            captcha_type = self._detect_captcha_type(driver)
            
            if not captcha_type:
                self.logger.debug("No CAPTCHA detected")
                return True
            
            self.logger.info(f"Detected CAPTCHA type: {captcha_type}")
            
            # Solve based on CAPTCHA type
            if captcha_type == "recaptcha":
                return self._solve_recaptcha(driver)
            elif captcha_type == "image_captcha":
                return self._solve_image_captcha(driver)
            elif captcha_type == "text_captcha":
                return self._solve_text_captcha(driver)
            else:
                self.logger.warning(f"Unsupported CAPTCHA type: {captcha_type}")
                return self._solve_manually(driver)
            
        except Exception as e:
            self.logger.error(f"Error solving CAPTCHA: {str(e)}")
            return False
    
    def _detect_captcha_type(self, driver: webdriver.Chrome) -> Optional[str]:
        """
        Detect the type of CAPTCHA
        
        :param driver: Selenium WebDriver instance
        :return: String identifier for CAPTCHA type
        """
        recaptcha_indicators = [
            "iframe[src*='recaptcha']",
            "iframe[src*='captcha']",
            "div.g-recaptcha",
            "div[data-sitekey]"
        ]
        
        for indicator in recaptcha_indicators:
            if len(driver.find_elements(By.CSS_SELECTOR, indicator)) > 0:
                return "recaptcha"
        
        image_captcha_indicators = [
            "img[src*='captcha']",
            "img[alt*='CAPTCHA']",
            "img[alt*='captcha']"
        ]
        
        for indicator in image_captcha_indicators:
            if len(driver.find_elements(By.CSS_SELECTOR, indicator)) > 0:
                return "image_captcha"
        
        text_captcha_indicators = [
            "input[name*='captcha']",
            "input[id*='captcha']",
            "label[for*='captcha']"
        ]
        
        for indicator in text_captcha_indicators:
            if len(driver.find_elements(By.CSS_SELECTOR, indicator)) > 0:
                return "text_captcha"
        
        return None
    
    def _solve_recaptcha(self, driver: webdriver.Chrome) -> bool:
        """
        Solve reCAPTCHA challenge
        
        :param driver: Selenium WebDriver instance
        :return: Boolean indicating success or failure
        """
        try:
            self.logger.info("Attempting to solve reCAPTCHA")
            
            if self.service_key:
                return self._solve_recaptcha_with_service(driver)
            
            return self._solve_recaptcha_with_audio(driver)
            
        except Exception as e:
            self.logger.error(f"Error solving reCAPTCHA: {str(e)}")
            return self._solve_manually(driver)
    
    def _solve_recaptcha_with_service(self, driver: webdriver.Chrome) -> bool:
        """
        Solve reCAPTCHA using external service
        
        :param driver: Selenium WebDriver instance
        :returns: Boolean indicating success or failure
        """
        # Depends on the specific API
        self.logger.info("External CAPTCHA solving service not implemented")
        return self._solve_manually(driver)
    
    def _solve_recaptcha_with_audio(self, driver: webdriver.Chrome) -> bool:
        """
        Attempt to solve reCAPTCHA using audio challenge
        
        :param driver: Selenium WebDriver instance
        :return: Boolean indicating success or failure
        """
        self.logger.info("Audio CAPTCHA solving not implemented")
        return self._solve_manually(driver)
    
    def _solve_image_captcha(self, driver: webdriver.Chrome) -> bool:
        """
        Solve an image-based CAPTCHA
        
        :param driver: Selenium WebDriver instance
        :return: Boolean indicating success or failure
        """
        try:
            self.logger.info("Attempting to solve image CAPTCHA")
            
            # Find CAPTCHA image
            captcha_img = driver.find_element(By.CSS_SELECTOR, "img[src*='captcha']")
            
            if not captcha_img:
                self.logger.error("CAPTCHA image not found")
                return False
            
            # Get image source
            img_src = captcha_img.get_attribute("src")
            
            # If have a service key, try using external service
            if self.service_key:
                solution = self._solve_image_with_service(img_src)
                if solution:
                    # Find input field
                    input_field = driver.find_element(By.CSS_SELECTOR, "input[name*='captcha']")
                    if input_field:
                        input_field.clear()
                        input_field.send_keys(solution)
                        
                        # Submit form
                        submit_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
                        if submit_button:
                            submit_button.click()
                            time.sleep(2) 
                            return True
            
            return self._solve_manually(driver)
            
        except Exception as e:
            self.logger.error(f"Error solving image CAPTCHA: {str(e)}")
            return self._solve_manually(driver)
    
    def _solve_image_with_service(self, img_src: str) -> Optional[str]:
        """
        Solve an image CAPTCHA using external service
        
        :param img_src: Source URL or data URI of the CAPTCHA image
        :return: Solution string or None if failed
        """
        self.logger.info("External image CAPTCHA solving not implemented")
        return None
    
    def _solve_text_captcha(self, driver: webdriver.Chrome) -> bool:
        """
        Solve a text-based CAPTCHA
        
        :param driver: Selenium WebDriver instance
        :return: Boolean indicating success or failure
        """
        try:
            self.logger.info("Attempting to solve text CAPTCHA")
            
            # Find the CAPTCHA question
            captcha_label = driver.find_element(By.CSS_SELECTOR, "label[for*='captcha']")
            
            if not captcha_label:
                self.logger.error("CAPTCHA prompt not found")
                return False
            
            # Get question text
            question = captcha_label.text
            self.logger.debug(f"CAPTCHA question: {question}")
            
            # Try to solve simple math problems
            solution = self._solve_math_captcha(question)
            
            if solution:
                # Find input field
                input_field = driver.find_element(By.CSS_SELECTOR, "input[name*='captcha']")
                if input_field:
                    input_field.clear()
                    input_field.send_keys(solution)
                    
                    # Submit form
                    submit_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
                    if submit_button:
                        submit_button.click()
                        time.sleep(2)
                        return True

            return self._solve_manually(driver)
            
        except Exception as e:
            self.logger.error(f"Error solving text CAPTCHA: {str(e)}")
            return self._solve_manually(driver)
    
    def _solve_math_captcha(self, question: str) -> Optional[str]:
        """
        Solve a simple math CAPTCHA
        
        :param question: The CAPTCHA question text
        :return: Solution string or None if not math problem
        """
        import re
        
        math_pattern = r"(\d+)\s*([\+\-\*\/])\s*(\d+)"
        match = re.search(math_pattern, question)
        
        if match:
            num1 = int(match.group(1))
            operator = match.group(2)
            num2 = int(match.group(3))
            
            if operator == "+":
                return str(num1 + num2)
            elif operator == "-":
                return str(num1 - num2)
            elif operator == "*":
                return str(num1 * num2)
            elif operator == "/" and num2 != 0:
                return str(num1 // num2)
        
        return None
    
    def _solve_manually(self, driver: webdriver.Chrome) -> bool:
        """
        Fallback method for manual CAPTCHA solution
        
        :param driver: Selenium WebDriver instance
        :return: Boolean indicating success or failure
        """
        self.logger.info("Falling back to manual CAPTCHA solution")
        
        try:
            # Screenshot to show CAPTCHA
            screenshot_path = os.path.join(os.getcwd(), "captcha_screenshot.png")
            driver.save_screenshot(screenshot_path)
            
            # Prompt for solution
            self.logger.info(f"CAPTCHA screenshot saved to {screenshot_path}")
            solution = input("Please solve the CAPTCHA and enter the solution: ")
            
            # Find input field
            input_fields = driver.find_elements(By.CSS_SELECTOR, "input[name*='captcha'], input[id*='captcha']")
            
            if input_fields:
                input_field = input_fields[0]
                input_field.clear()
                input_field.send_keys(solution)
                
                # Try to find and click submit button
                submit_buttons = driver.find_elements(By.CSS_SELECTOR, 
                                                     "button[type='submit'], input[type='submit'], .submit-button")
                
                if submit_buttons:
                    submit_buttons[0].click()
                    time.sleep(2)
                    return True
                else:
                    self.logger.warning("Submit button not found")
                    return False
            else:
                self.logger.warning("CAPTCHA input field not found")
                return False
                
        except Exception as e:
            self.logger.error(f"Error during manual CAPTCHA solution: {str(e)}")
            return False
        