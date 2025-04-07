"""
BigQuery Loader Module

Functionality to load transaction data into BigQuery 
for storage and analysis. Handles schema management,
incremental loading, and data type conversions
"""

import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from pandas import DataFrame

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from src.models.transaction import Transaction
from src.utils.config import ConfigManager
from src.utils.logger import get_logger


class BigQueryLoader:
    """
    Class for loading transaction data into BigQuery
    
    Provides ways to load processed transaction data into
    Google BigQuery, handling schema alignment, incremental loading logic,
    and data transformations
    """
    
    def __init__(self):
        """Initialize BigQuery loader"""
        self.logger = get_logger("bigquery_loader")
        self.config_manager = ConfigManager()
        
        # Load BigQuery config
        config = self.config_manager.get_bigquery_config()
        self.project_id = config.get("project_id")
        self.dataset_id = config.get("dataset_id")
        self.transactions_table = config.get("transactions_table", "transactions")
        self.client = None
        
        # Initialize client
        self._init_client()
    
    def _init_client(self) -> None:
        """Initialize the BigQuery client"""
        try:
            self.client = bigquery.Client(project=self.project_id)
            self.logger.info(f"Initialized BigQuery client for project {self.project_id}")
        except Exception as e:
            self.logger.error(f"Error initializing BigQuery client: {str(e)}")
            raise
    
    def get_latest_transaction_date(self) -> Optional[datetime]:
        """
        Get the latest transaction date from BigQuery
        
        :return: The latest transaction date
        """
        try:
            query = f"""
                SELECT MAX(date) as max_date
                FROM `{self.project_id}.{self.dataset_id}.{self.transactions_table}`
            """

            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                return row.max_date

            return None
            
        except NotFound:
            # Table doesn't exist yet
            self.logger.info(f"Table {self.transactions_table} not found, returning None")
            return None
        except Exception as e:
            self.logger.error(f"Error getting latest transaction date: {str(e)}")
            return None
    
    def load(self, transactions: List[Transaction]) -> bool:
        """
        Load transactions into BigQuery
        
        :param transactions: List of transaction objects to load
        :return: Boolean indicating success or failure
        """
        if not transactions:
            self.logger.info("No transactions to load")
            return True
        
        try:
            self.logger.info(f"Loading {len(transactions)} transactions into BigQuery")
            
            # Convert to DataFrame
            df = self._transactions_to_dataframe(transactions)
            
            # Check dataset exists
            self._ensure_dataset_exists()
            
            # Check table exists
            self._ensure_table_exists(df)
            
            # Get existing transaction IDs and filter out
            existing_ids = self._get_existing_transaction_ids([t.transaction_id for t in transactions])
            new_transactions = df[~df['transaction_id'].isin(existing_ids)]
            
            if new_transactions.empty:
                self.logger.info("All transactions already exist in BigQuery")
                return True
            
            # Load new transactions
            table_ref = f"{self.project_id}.{self.dataset_id}.{self.transactions_table}"
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ]
            )
            load_job = self.client.load_table_from_dataframe(
                new_transactions, table_ref, job_config=job_config
            )

            load_job.result()
            
            self.logger.info(f"Successfully loaded {len(new_transactions)} new transactions")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading transactions to BigQuery: {str(e)}", exc_info=True)
            return False
    
    def _transactions_to_dataframe(self, transactions: List[Transaction]) -> DataFrame:
        """
        Convert transaction objects to a BigQuery-compatible DataFrame
        
        :param transactions: List of transaction objects
        :return: DataFrame with BigQuery-compatible data types
        """
        # Convert to dictionaries
        rows = [t.to_bigquery_row() for t in transactions]
        
        df = pd.DataFrame(rows)
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        bool_columns = ['is_recurring', 'is_transfer', 'is_income', 
                        'is_reimbursable', 'is_ignored']
        for col in bool_columns:
            if col in df.columns:
                df[col] = df[col].astype(bool)
        
        numeric_columns = ['amount']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)
        
        if 'metadata' in df.columns:
            df['metadata'] = df['metadata'].apply(
                lambda x: json.dumps(x) if isinstance(x, dict) else x
            )
        
        return df
    
    def _ensure_dataset_exists(self) -> None:
        """
        Ensure BigQuery dataset exists, creating it if necessary

        :raises Exception: If dataset creation fails
        """
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            
            try:
                self.client.get_dataset(dataset_ref)
                self.logger.debug(f"Dataset {self.dataset_id} already exists")
            except NotFound:
                # Create dataset
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"
                self.client.create_dataset(dataset)
                self.logger.info(f"Created dataset {self.dataset_id}")
        except Exception as e:
            self.logger.error(f"Error ensuring dataset exists: {str(e)}")
            raise
    
    def _ensure_table_exists(self, df: DataFrame) -> None:
        """
        Ensure BigQuery table exists, creating it if necessary
        
        :param df: DataFrame with the schema to use for table creation
        :raises Exception: If table creation fails
        """
        try:
            table_ref = f"{self.project_id}.{self.dataset_id}.{self.transactions_table}"
            
            try:
                self.client.get_table(table_ref)
                self.logger.debug(f"Table {self.transactions_table} already exists")
            except NotFound:
                schema = self._generate_schema_from_dataframe(df)
                table = bigquery.Table(table_ref, schema=schema)

                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="date"
                )
                table.clustering_fields = ["source", "category", "account_type"]
                
                self.client.create_table(table)
                self.logger.info(f"Created table {self.transactions_table}")
                
        except Exception as e:
            self.logger.error(f"Error ensuring table exists: {str(e)}")
            raise
    
    def _generate_schema_from_dataframe(self, df: DataFrame) -> List[bigquery.SchemaField]:
        """
        Generate BigQuery schema from DataFrame
        
        :param df: DataFrame to generate schema from
        :return: List of BigQuery SchemaField objects
        """
        schema = []
        
        dtype_map = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'object': 'STRING'
        }
        
        core_fields = {
            'transaction_id': bigquery.SchemaField('transaction_id', 'STRING', mode='REQUIRED'),
            'date': bigquery.SchemaField('date', 'TIMESTAMP', mode='REQUIRED'),
            'amount': bigquery.SchemaField('amount', 'FLOAT', mode='REQUIRED'),
            'description': bigquery.SchemaField('description', 'STRING', mode='REQUIRED'),
            'source': bigquery.SchemaField('source', 'STRING', mode='REQUIRED'),
            'account_type': bigquery.SchemaField('account_type', 'STRING', mode='REQUIRED'),
            'category': bigquery.SchemaField('category', 'STRING', mode='NULLABLE'),
            'subcategory': bigquery.SchemaField('subcategory', 'STRING', mode='NULLABLE'),
            'is_recurring': bigquery.SchemaField('is_recurring', 'BOOLEAN', mode='NULLABLE'),
            'is_transfer': bigquery.SchemaField('is_transfer', 'BOOLEAN', mode='NULLABLE'),
            'is_income': bigquery.SchemaField('is_income', 'BOOLEAN', mode='NULLABLE'),
            'is_reimbursable': bigquery.SchemaField('is_reimbursable', 'BOOLEAN', mode='NULLABLE'),
            'is_ignored': bigquery.SchemaField('is_ignored', 'BOOLEAN', mode='NULLABLE'),
            'metadata': bigquery.SchemaField('metadata', 'STRING', mode='NULLABLE')
        }
        
        # Add core fields that exist
        for field_name, schema_field in core_fields.items():
            if field_name in df.columns:
                schema.append(schema_field)
        
        # Add any additional fields
        for col in df.columns:
            if col not in core_fields:
                col_type = str(df[col].dtype)
                bq_type = dtype_map.get(col_type, 'STRING')
                schema.append(bigquery.SchemaField(col, bq_type, mode='NULLABLE'))
        
        return schema
    
    def _get_existing_transaction_ids(self, transaction_ids: List[str]) -> List[str]:
        """
        Get list of transaction IDs that already exist in BigQuery
        
        :param transaction_ids: List of transaction IDs to check
        :return: List of transaction IDs that already exist
        """
        if not transaction_ids:
            return []
        
        try:
            query = f"""
                SELECT transaction_id
                FROM `{self.project_id}.{self.dataset_id}.{self.transactions_table}`
                WHERE transaction_id IN UNNEST(@transaction_ids)
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("transaction_ids", "STRING", transaction_ids)
                ]
            )

            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            existing_ids = [row.transaction_id for row in results]
            
            self.logger.debug(f"Found {len(existing_ids)} existing transactions in BigQuery")
            return existing_ids
            
        except NotFound:
            # Table doesn't exist yet
            self.logger.debug(f"Table {self.transactions_table} not found, no existing transactions")
            return []
        except Exception as e:
            self.logger.error(f"Error checking existing transaction IDs: {str(e)}")
            return []
