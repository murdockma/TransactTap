"""
Selenium-Based Extractor

Base implementation of the BaseExtractor using Selenium. 
Implements browser interaction patterns used across different bank extractors
"""

import os
import time
import random
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple, Union
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    ElementClickInterceptedException,
    NoSuchElementException,
    StaleElementReferenceException
)
from webdriver_manager.chrome import ChromeDriverManager

from src.extractors.base_extractor import BaseExtractor
from src.models.transaction import Transaction
from src.auth.captcha_solver import CaptchaSolver


class SeleniumExtractor(BaseExtractor):
    """
    Base class for Selenium-based bank data extractors
    
    Extends BaseExtractor for Selenium-specific functionality
    for web scraping. Handles common Selenium tasks like
    driver initialization, element finding, and waiting
    """
    
    def __init__(self, bank_id: str, config: Dict[str, Any]):
        """
        Initialize Selenium extractor
        
        :param bank_id: Unique identifier for the bank
        :param config: Configuration dictionary for bank
        """
        super().__init__(bank_id, config)
        
        # Selenium-specific config
        self.headless = config.get("headless", True)
        self.download_dir = config.get("download_dir", os.path.join("data", "raw", bank_id))
        self.driver = None
        self.wait = None
        self.captcha_solver = CaptchaSolver()
        
        # Create download dir
        os.makedirs(self.download_dir, exist_ok=True)
    
    def _init_driver(self) -> None:
        """Initialize and configure the WebDriver"""
        self.logger.debug("Initializing Selenium WebDriver...")
        
        # Set up Chrome options
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Set up download behavior
        prefs = {
            "download.default_directory": os.path.abspath(self.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Initialize chrome driver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 15)
        
        self.logger.debug("Selenium WebDriver initialized successfully")
    
    def _cleanup_driver(self) -> None:
        """Clean up and quit WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                self.logger.error(f"Error quitting WebDriver: {str(e)}")
            finally:
                self.driver = None
                self.wait = None
    
    def find_element(self, selector: str, by: By = By.CSS_SELECTOR, 
                    timeout: int = 15, retries: int = 3) -> Optional[webdriver.remote.webelement.WebElement]:
        """
        Find an element
        
        :param selector: CSS selector or XPath expression
        :param by: Selenium By strategy (e.g., By.CSS_SELECTOR, By.XPATH)
        :param timeout: Timeout in seconds
        :param retries: Number of retry attempts
        :return: WebElement if found
        """
        for attempt in range(retries):
            try:
                return WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((by, selector))
                )
            except TimeoutException:
                if attempt < retries - 1:
                    self.logger.debug(f"Retrying to find element {selector} (attempt {attempt+1}/{retries})")
                    time.sleep(random.uniform(0.5, 2.0))
                else:
                    self.logger.error(f"Timed out waiting for element: {selector}")
                    return None
            except Exception as e:
                self.logger.error(f"Error finding element {selector}: {str(e)}")
                return None
    
    def click_element(self, element_or_selector: Union[str, webdriver.remote.webelement.WebElement], 
                     by: By = By.CSS_SELECTOR, timeout: int = 15, 
                     retry_on_intercept: bool = True) -> bool:
        """
        Safely click an element
        
        :param element_or_selector: Either a WebElement or a selector string
        :param by: Selenium By strategy if selector is provided
        :param timeout: Timeout in seconds for element to be clickable
        :param retry_on_intercept: Whether to retry if click is intercepted
        :return: Boolean indicating success or failure
        """
        try:
            # Get the element if selector provided
            if isinstance(element_or_selector, str):
                element = WebDriverWait(self.driver, timeout).until(
                    EC.element_to_be_clickable((by, element_or_selector))
                )
            else:
                element = element_or_selector
                WebDriverWait(self.driver, timeout).until(
                    EC.element_to_be_clickable((by, element))
                )
            
            # Try regular click first
            try:
                element.click()
                return True
            except ElementClickInterceptedException:
                if retry_on_intercept:
                    self.logger.debug("Click intercepted, trying JavaScript click...")
                    self.driver.execute_script("arguments[0].click();", element)
                    return True
                else:
                    raise
            
        except TimeoutException:
            self.logger.error(f"Timed out waiting for element to be clickable")
        except StaleElementReferenceException:
            self.logger.error("Element reference is stale")
        except Exception as e:
            self.logger.error(f"Error clicking element: {str(e)}")
        
        return False
    
    def type_text(self, selector: str, text: str, by: By = By.CSS_SELECTOR, 
                 clear_first: bool = True, timeout: int = 15) -> bool:
        """
        Type text into an input field
        
        :param selector: CSS selector or XPath expression
        :param text: Text to type
        :param by: Selenium By strategy
        :param clear_first: Whether to clear the field first
        :param timeout: Timeout in seconds
        :return: Boolean indicating success or failure
        """
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, selector))
            )
            
            if clear_first:
                element.clear()
            
            for char in text:
                element.send_keys(char)
                time.sleep(random.uniform(0.01, 0.1)) 
            
            return True
            
        except TimeoutException:
            self.logger.error(f"Timed out waiting for input field: {selector}")
        except Exception as e:
            self.logger.error(f"Error typing text: {str(e)}")
        
        return False
    
    def wait_for_download(self, timeout: int = 60, check_interval: float = 1.0) -> Optional[Path]:
        """
        Wait for a file to be downloaded and return path
        
        :param timeout: Maximum time to wait in seconds
        :param check_interval: How often to check for new files
        :return: Path to the downloaded file
        """
        self.logger.debug(f"Waiting for download in {self.download_dir}...")
        
        # Get initial files
        before = set(Path(self.download_dir).glob("*"))
        
        # Wait for file to appear
        start_time = time.time()
        while time.time() - start_time < timeout:
            time.sleep(check_interval)
            
            # Get current files
            after = set(Path(self.download_dir).glob("*"))
            new_files = after - before
            
            # Check for .crdownload or similar partial files
            partial_downloads = [f for f in new_files if f.name.endswith('.crdownload') or 
                                 f.name.endswith('.part') or f.name.endswith('.tmp')]
            
            if partial_downloads:
                self.logger.debug("Download in progress...")
                continue
                
            # Look for actual completed downloads
            completed_downloads = [f for f in new_files if f not in partial_downloads]
            if completed_downloads:
                newest_file = max(completed_downloads, key=lambda f: f.stat().st_mtime)
                self.logger.debug(f"Download completed: {newest_file}")
                return newest_file
        
        self.logger.error(f"Download timed out after {timeout} seconds")
        return None
    
    def handle_captcha(self, frame_selector: Optional[str] = None) -> bool:
        """
        Handle CAPTCHA challenges
        
        :param frame_selector: Optional selector for iframe containing CAPTCHA
        :return: Boolean indicating success or failure
        """
        try:
            self.logger.info("CAPTCHA detected, attempting to solve...")
            
            # Switch to frame i f selector provided
            if frame_selector:
                frame = self.find_element(frame_selector)
                if frame:
                    self.driver.switch_to.frame(frame)
            
            # Try captcha solver
            captcha_solved = self.captcha_solver.solve_captcha(self.driver)
            
            # Switch back to main content
            if frame_selector:
                self.driver.switch_to.default_content()
            
            return captcha_solved
            
        except Exception as e:
            self.logger.error(f"Error handling CAPTCHA: {str(e)}")
            
            # Make sure we're back in main content
            try:
                self.driver.switch_to.default_content()
            except Exception:
                pass
                
            return False
    
    def login(self) -> bool:
        """Default implementation to be overridden by specific bank extractors"""
        self.logger.warning("Using default login implementation. This should be overridden.")
        return False
    
    def navigate_to_transactions(self, account_type: Optional[str] = None) -> bool:
        """Default implementation to be overridden by specific bank extractors"""
        self.logger.warning("Using default navigate_to_transactions implementation. This should be overridden.")
        return False
    
    def download_transactions(self, start_date: datetime, end_date: datetime) -> List[Transaction]:
        """Default implementation to be overridden by specific bank extractors"""
        self.logger.warning("Using default download_transactions implementation. This should be overridden.")
        return []
    
    def logout(self) -> bool:
        """
        Default implementation of logout with common patterns.
        Can be overridden by bank extractors
        """
        self.logger.debug("Attempting to logout...")
        
        try:
            # Common logout patterns
            logout_selectors = [
                "a.logout", "button.logout", 
                "a[href*='logout']", "button[aria-label*='Log Out']",
                "//a[contains(text(), 'Log Out')]", "//a[contains(text(), 'Sign Out')]"
            ]
            
            # Try each selector
            for selector in logout_selectors:
                by = By.XPATH if selector.startswith("//") else By.CSS_SELECTOR
                logout_element = self.find_element(selector, by=by, timeout=5)
                
                if logout_element:
                    self.click_element(logout_element, by=by)
                    time.sleep(2) 
                    self.logger.debug("Logout successful")
                    return True
            
            self.logger.warning("Could not find logout button, closing browser instead")
            return False
            
        except Exception as e:
            self.logger.error(f"Error during logout: {str(e)}")
            return False
        finally:
            self._cleanup_driver()
    
    def extract(self, start_date: datetime, end_date: datetime) -> List[Transaction]:
        """
        Implementation of the extract method from BaseExtractor that initializes
        the Selenium driver before extraction.
        """
        try:
            # Initialize web driver and call parent extract
            self._init_driver()
            return super().extract(start_date, end_date)
        finally:
            self._cleanup_driver()
