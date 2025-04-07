"""
Transaction Data Model

Defines Transaction class, which represents a financial transaction
from a source. Includes methods for working with transaction data, like
serialization, categorization, and comparison
"""

import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List, Union
from dataclasses import dataclass, field, asdict
import json
import re


@dataclass
class Transaction:
    """
    Represents a financial transaction
    
    Models a financial transaction from any source, with methods
    for categorization, serialization, and data manipulation
    """
    
    # Required fields
    date: datetime
    amount: float
    description: str
    
    # Optional fields with defaults
    transaction_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    account_type: str = "unknown"
    source: str = "unknown"
    category: Optional[str] = None
    subcategory: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Flags
    is_recurring: bool = False
    is_transfer: bool = False
    is_income: bool = False
    is_reimbursable: bool = False
    is_ignored: bool = False
    
    def __post_init__(self):
        """Validate and process fields after initialization"""
        if isinstance(self.date, str):
            self.date = datetime.fromisoformat(self.date.replace('Z', '+00:00'))
        
        self.amount = float(self.amount)
        
        # Determine if income based on amount
        if self.amount > 0:
            self.is_income = True
        
        self.description = self.description.strip()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert transaction to a dictionary
        
        :return: Dictionary of transaction
        """
        result = asdict(self)
        result['date'] = self.date.isoformat()
        
        return result
    
    def to_json(self) -> str:
        """
        Convert transaction to JSON string
        
        :return: JSON string of the transaction
        """
        return json.dumps(self.to_dict())
    
    def to_bigquery_row(self) -> Dict[str, Any]:
        """
        Convert transaction to BigQuery-compatible row format
        
        :return: Dictionary formatted for BigQuery insertion
        """
        row = self.to_dict()
        row['date'] = self.date.isoformat()
        
        if self.metadata:
            row['metadata'] = json.dumps(self.metadata)
        
        return row
    
    def matches(self, other: 'Transaction', fuzzy: bool = False) -> bool:
        """
        Check if transaction matches another transaction
        
        :param other: Another Transaction to compare with
        :param fuzzy: Whether to use fuzzy matching criteria
        :return: Boolean indicating whether transactions match
        """
        # For exact, check key fields
        if not fuzzy:
            return (
                self.date.date() == other.date.date() and
                abs(self.amount - other.amount) < 0.01 and
                self.description == other.description
            )
        
        # For fuzzy, allow some variation
        date_diff = abs((self.date.date() - other.date.date()).days)
        amount_diff_pct = abs((self.amount - other.amount) / self.amount) if self.amount != 0 else float('inf')
        
        return (
            date_diff <= 2 and
            amount_diff_pct < 0.05 and
            (self.description in other.description or other.description in self.description)
        )
    
    def categorize(self, category_mappings: Dict[str, Dict[str, str]]) -> None:
        """
        Categorize transaction based on mappings
        
        :param category_mappings: Dictionary mapping regex patterns to categories/subcategories
        """
        if self.category and self.subcategory:
            return
        
        desc_lower = self.description.lower()
        
        for pattern, category_info in category_mappings.items():
            if re.search(pattern.lower(), desc_lower):
                self.category = category_info.get('category')
                self.subcategory = category_info.get('subcategory')
                return
        
        # Default categorization
        self.category = "Uncategorized"
        self.subcategory = None
    
    def detect_recurring(self, transactions: List['Transaction']) -> bool:
        """
        Detect if a recurring transaction using transaction history
        
        :param transactions: List of historical transactions to analyze
        :return: Boolean indicating whether this is recurring
        """
        # Look for similar transactions
        similar_transactions = []
        
        for tx in transactions:
            if tx.transaction_id == self.transaction_id:
                continue
            
            # Check for similar amount and description
            if (abs(tx.amount - self.amount) < 0.01 and
                self._description_similarity(tx.description, self.description) > 0.8):
                similar_transactions.append(tx)
        
        similar_transactions.sort(key=lambda x: x.date)
        
        # If we have at least 2 similar transactions, check for patterns
        if len(similar_transactions) >= 2:
            # Monthly pattern (28-31 days apart)
            intervals = []
            for i in range(1, len(similar_transactions)):
                days = (similar_transactions[i].date - similar_transactions[i-1].date).days
                intervals.append(days)
            
            # Consistent intervals
            if all(27 <= interval <= 33 for interval in intervals):
                self.is_recurring = True
                return True
        
        return False
    
    def _description_similarity(self, desc1: str, desc2: str) -> float:
        """
        Calculate similarity between two transaction descriptions
        
        :param desc1: First description
        :param desc2: Second description
        :return: Similarity score between 0 and 1
        """
        # Basic word overlap ratio
        words1 = set(desc1.lower().split())
        words2 = set(desc2.lower().split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        return len(intersection) / len(union)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Transaction':
        """
        Create transaction from a dictionary
        
        :param data: Dictionary with transaction data
        :return: Transaction instance
        """
        if 'date' in data and isinstance(data['date'], str):
            data['date'] = datetime.fromisoformat(data['date'].replace('Z', '+00:00'))
        
        if 'metadata' in data and isinstance(data['metadata'], str):
            data['metadata'] = json.loads(data['metadata'])
        
        return cls(**data)
    
    @classmethod
    def from_csv_row(cls, row: Dict[str, Any], source: str, account_type: str) -> 'Transaction':
        """
        Create transaction from CSV row
        
        :param row: Dictionary representing a CSV row
        :param source: Source identifier (e.g., 'chase', 'wells_fargo')
        :param account_type: Type of account (e.g., 'checking', 'credit')
        :return: Transaction instance
        """
        # Common CSV fields
        field_mappings = {
            'date': ['date', 'transaction_date', 'post_date', 'Date'],
            'amount': ['amount', 'transaction_amount', 'Amount'],
            'description': ['description', 'merchant', 'memo', 'Description']
        }
        
        data = {}
        
        for field, csv_fields in field_mappings.items():
            for csv_field in csv_fields:
                if csv_field in row:
                    data[field] = row[csv_field]
                    break

        data['source'] = source
        data['account_type'] = account_type
        
        return cls(**data)
    