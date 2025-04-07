"""
Configuration Manager Module

Functionality to load and manage configuration settings
for the pipeline. Handles loading configs from files,
env variables, and provides access to specific config sections
"""

import os
import yaml
import json
from typing import Dict, Any, List, Optional
from pathlib import Path

from dotenv import load_dotenv

from src.utils.logger import get_logger


class ConfigManager:
    """
    Configuration manager for the finance data pipeline
    
    Provides ways to load and access configuration settings
    from various sources including YAML files, JSON files, and env vars
    """
    
    def __init__(self, config_dir: str = None):
        """
        Initialize config manager
        
        :param config_dir: Optional directory path for configuration files
        """
        self.logger = get_logger("config_manager")

        load_dotenv()

        if config_dir:
            self.config_dir = Path(config_dir)
        else:
            self.config_dir = Path(__file__).parents[2] / "config"
        
        self._config_cache = {}
        self._bank_configs = {}
        self._category_mappings = None
    
    def load_config(self) -> Dict[str, Any]:
        """
        Load main configuration settings
        
        :return: Dictionary of configuration settings
        """
        if "main" in self._config_cache:
            return self._config_cache["main"]
        
        try:
            # Load main settings
            settings_file = self.config_dir / "settings.yaml"
            
            if not settings_file.exists():
                self.logger.warning(f"Settings file not found: {settings_file}")
                self._config_cache["main"] = {}
                return {}
            
            with open(settings_file, "r") as f:
                config = yaml.safe_load(f)
            
            self._config_cache["main"] = config
            
            self.logger.debug("Loaded main configuration")
            return config
            
        except Exception as e:
            self.logger.error(f"Error loading configuration: {str(e)}")
            self._config_cache["main"] = {}
            return {}
    
    def get_bank_config(self, bank_id: str) -> Dict[str, Any]:
        """
        Get configuration for bank
        
        :param bank_id: Identifier for the bank
        :return: Dictionary of bank-specific configuration settings
        """
        if bank_id in self._bank_configs:
            return self._bank_configs[bank_id]
        
        try:
            bank_config_file = self.config_dir / "banks" / f"{bank_id.lower()}.yaml"
            
            if not bank_config_file.exists():
                self.logger.warning(f"Config file not found for bank: {bank_id}")
                self._bank_configs[bank_id] = {}
                return {}
            
            with open(bank_config_file, "r") as f:
                config = yaml.safe_load(f)
            
            username_env = f"{bank_id.upper()}_USERNAME"
            password_env = f"{bank_id.upper()}_PASSWORD"
            
            if username_env in os.environ:
                config["username"] = os.environ[username_env]
            
            if password_env in os.environ:
                config["password"] = os.environ[password_env]
            
            self._bank_configs[bank_id] = config
            
            self.logger.debug(f"Loaded configuration for bank: {bank_id}")
            return config
            
        except Exception as e:
            self.logger.error(f"Error loading bank configuration for {bank_id}: {str(e)}")
            self._bank_configs[bank_id] = {}
            return {}
    
    def get_category_mappings(self) -> Dict[str, Dict[str, str]]:
        """
        Get transaction category mappings
        
        :return: Dictionary mapping patterns to category information
        """
        if self._category_mappings is not None:
            return self._category_mappings
        
        try:
            mappings_file = self.config_dir / "mappings.json"
            
            if not mappings_file.exists():
                self.logger.warning(f"Mappings file not found: {mappings_file}")
                self._category_mappings = {}
                return {}
            
            with open(mappings_file, "r") as f:
                mappings = json.load(f)
            
            self._category_mappings = mappings
            
            self.logger.debug(f"Loaded {len(mappings)} category mappings")
            return mappings
            
        except Exception as e:
            self.logger.error(f"Error loading category mappings: {str(e)}")
            self._category_mappings = {}
            return {}
    
    def get_bigquery_config(self) -> Dict[str, Any]:
        """
        Get BigQuery config settings
        
        Returns:
            Dictionary of BigQuery configuration settings
        """
        config = self.load_config()
        bq_config = config.get("bigquery", {})
        
        if "BIGQUERY_PROJECT_ID" in os.environ:
            bq_config["project_id"] = os.environ["BIGQUERY_PROJECT_ID"]
        
        if "BIGQUERY_DATASET_ID" in os.environ:
            bq_config["dataset_id"] = os.environ["BIGQUERY_DATASET_ID"]
        
        return bq_config
    
    def get_bank_list(self) -> List[str]:
        """
        Get list of configured banks
        
        :return: List of bank identifiers
        """
        try:
            # Look for bank config files
            banks_dir = self.config_dir / "banks"
            
            if not banks_dir.exists():
                self.logger.warning(f"Banks directory not found: {banks_dir}")
                return []
            
            # Get all YAML files
            bank_files = list(banks_dir.glob("*.yaml"))
            
            # Extract bank IDs
            bank_ids = [f.stem for f in bank_files]
            
            self.logger.debug(f"Found {len(bank_ids)} configured banks")
            return bank_ids
            
        except Exception as e:
            self.logger.error(f"Error getting bank list: {str(e)}")
            return []
        