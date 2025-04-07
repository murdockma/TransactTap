"""
Extractor Factory Module

Factory for creating the appropriate bank extractor
based on the bank identifier
"""

from typing import Dict, Any, Optional

from src.extractors.base_extractor import BaseExtractor
from src.extractors.wells_fargo_extractor import WellsFargoExtractor
from src.extractors.chase_extractor import ChaseExtractor
from src.utils.config import ConfigManager


class ExtractorFactory:
    """
    Factory class for creating bank data extractors
    
    Creates and returns appropriate extractor based 
    on bank identifier. Also handles loading and 
    providing config to each extractor
    """
    
    def __init__(self):
        """Initialize the extractor factory with config"""
        self.config_manager = ConfigManager()
        self.extractors = {} 
    
    def get_extractor(self, bank_id: str) -> BaseExtractor:
        """
        Get appropriate extractor for bank
        
        :param bank_id: Identifier for bank
        :return: Instance of a BaseExtractor subclass
        :raises ValueError: If bank_id is not supported
        """
        # Return cached extractor if exists
        if bank_id in self.extractors:
            return self.extractors[bank_id]
        
        # Load bank-specific config and create extractor
        bank_config = self.config_manager.get_bank_config(bank_id)
        extractor = self._create_extractor(bank_id, bank_config)
        
        self.extractors[bank_id] = extractor
        
        return extractor
    
    def _create_extractor(self, bank_id: str, config: Dict[str, Any]) -> BaseExtractor:
        """
        Create new extractor instance based on bank_id
        
        :param bank_id: Identifier for bank
        :param config: Configuration for bank
        :return: Instance of a BaseExtractor subclass
        :raises ValueError: If bank_id is not supported
        """
        # Map bank IDs to extractor classes
        extractors = {
            "wells_fargo": WellsFargoExtractor,
            "chase": ChaseExtractor,
            # Add more banks here as they're implemented
        }
        
        # Get extractor class
        extractor_class = extractors.get(bank_id.lower())
        
        if not extractor_class:
            raise ValueError(f"Unsupported bank: {bank_id}")
        
        # Create and return instance of extractor
        return extractor_class(config)
