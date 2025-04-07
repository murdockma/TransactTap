"""
Extractors package

Modules for extracting transaction data from financial institutions
"""

from src.extractors.base_extractor import BaseExtractor
from src.extractors.selenium_extractor import SeleniumExtractor
from src.extractors.wells_fargo_extractor import WellsFargoExtractor
from src.extractors.chase_extractor import ChaseExtractor
from src.extractors.extractor_factory import ExtractorFactory

__all__ = [
    'BaseExtractor',
    'SeleniumExtractor',
    'WellsFargoExtractor',
    'ChaseExtractor',
    'ExtractorFactory',
]
