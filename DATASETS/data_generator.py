"""
FinBank Synthetic Data Generator
Author: SINGOEI RODGERS
Date: 26/09/2025

Description: 
This script generates a comprehensive synthetic banking dataset that mirrors real-world 
financial data patterns. The dataset is designed for end-to-end data analysis projects 
including data warehousing, EDA, advanced analytics, and reporting.

Features:
- Realistic customer demographics and behavior patterns
- Temporal transaction patterns (seasonality, time-of-day effects)
- Account lifecycle simulations
- Loan and credit card data with proper financial calculations
- Geographic distribution across major US cities

Usage:
- Run as standalone script to generate complete dataset
- Import specific generators for custom data needs
- Adjust parameters in __init__ for dataset scale
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
from typing import List, Dict, Tuple, Optional
import uuid
import os

class BankingDataGenerator:
    """
    A comprehensive synthetic banking data generator that creates realistic financial datasets.
    
    This class generates interconnected tables including customers, accounts, transactions,
    loans, credit cards, and branch information with realistic relationships and patterns.
    
    Attributes:
        fake (Faker): Faker instance for generating realistic fake data
        customer_count (int): Number of customers to generate
        transaction_years (int): Number of years of transaction data
        start_date (datetime): Start date for transaction history
        end_date (datetime): End date for transaction history
    """
    
    def __init__(self, seed: int = 42, customer_count: int = 10000, transaction_years: int = 5):
        """
        Initialize the data generator with configuration parameters.
        
        Args:
            seed (int): Random seed for reproducible results
            customer_count (int): Number of customers to generate
            transaction_years (int): Number of years of transaction history
        """
        # Initialize Faker and set seeds for reproducibility
        self.fake = Faker()
        self.fake.seed_instance(seed)
        np.random.seed(seed)
        random.seed(seed)
        
        # Configuration parameters
        self.customer_count = customer_count
        self.transaction_years = transaction_years
        self.start_date = datetime(2021, 1, 1)
        self.end_date = datetime(2025, 12, 31)
        
        # Realistic constants for data generation
        self.BRANCH_CITIES = {
            'New York': (40.7128, -74.0060), 'Los Angeles': (34.0522, -118.2437),
            'Chicago': (41.8781, -87.6298), 'Houston': (29.7604, -95.3698),
            'Phoenix': (33.4484, -112.0740), 'Philadelphia': (39.9526, -75.1652),
            'San Antonio': (29.4241, -98.4936), 'San Diego': (32.7157, -117.1611),
            'Dallas': (32.7767, -96.7970), 'San Jose': (37.3382, -121.8863),
            'Austin': (30.2672, -97.7431), 'Jacksonville': (30.3322, -81.6557),
            'Fort Worth': (32.7555, -97.3308), 'Columbus': (39.9612, -82.9988),
            'San Francisco': (37.7749, -122.4194), 'Seattle': (47.6062, -122.3321),
            'Denver': (39.7392, -104.9903), 'Boston': (42.3601, -71.0589),
            'Atlanta': (33.7490, -84.3880), 'Miami': (25.7617, -80.1918)
        }

    # ============================================================================
    # BRANCH DATA GENERATION
    # ============================================================================
    
    def generate_branches(self) -> pd.DataFrame:
        """
        Generate branch locations with realistic geographic distribution.
        
        Creates branch data including location details, opening dates, and operational metrics.
        Branches are distributed across major US cities with realistic coordinates.
        
        Returns:
            pd.DataFrame: Branches data with columns:
                - branch_id, branch_name, city, state, zip_code
                - latitude, longitude, opening_date, total_deposits, employee_count
        """
        branches = []
        
        for i, (city, coords) in enumerate(self.BRANCH_CITIES.items()):
            branch = {
                'branch_id': f'BR{i+1:04d}',
                'branch_name': f'{city} Main Branch',
                'city': city,
                'state': self.fake.state_abbr(),
                'zip_code': self.fake.zipcode(),
                'latitude': coords[0] + random.uniform(-0.1, 0.1),  # Add slight variation
                'longitude': coords[1] + random.uniform(-0.1, 0.1),
                'opening_date': self.fake.date_between(start_date='-20y', end_date='-1y'),
                'total_deposits': random.randint(50_000_000, 500_000_000),  # $50M-$500M range
                'employee_count': random.randint(15, 100)
            }
            branches.append(branch)
        
        return pd.DataFrame(branches)

    # ============================================================================
    # CUSTOMER DATA GENERATION
    # ============================================================================
    
    def generate_customers(self, branch_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate customer profiles with realistic demographics and financial characteristics.
        
        Creates diverse customer base with age-appropriate income levels, credit scores,
        and employment status. Customers are assigned to branches based on geographic logic.
        
        Args:
            branch_df (pd.DataFrame): Branches data for customer-branch assignment
            
        Returns:
            pd.DataFrame: Customers data with demographic and financial attributes
        """
        customers = []
        
        # Realistic age distribution weights for US population
        AGE_DISTRIBUTION = {
            '18-25': 0.15, '26-35': 0.25, '36-45': 0.20,
            '46-55': 0.15, '56-65': 0.15, '65+': 0.10
        }
        
        # Age-appropriate income ranges (annual, in USD)
        INCOME_RANGES = {
            '18-25': (20_000, 60_000), '26-35': (35_000, 90_000),
            '36-45': (50_000, 150_000), '46-55': (60_000, 180_000),
            '56-65': (55_000, 160_000), '65+': (30_000, 100_000)
        }
        
        for i in range(self.customer_count):
            # Select age group based on realistic distribution
            age_group = random.choices(
                list(AGE_DISTRIBUTION.keys()), 
                weights=list(AGE_DISTRIBUTION.values())
            )[0]
            
            # Calculate age and birth date
            if '+' in age_group:
                min_age, max_age = 65, 90
            else:
                min_age, max_age = map(int, age_group.split('-'))
                
            age = random.randint(min_age, max_age)
            birth_date = datetime.now() - timedelta(days=age * 365 + random.randint(0, 364))
            
            # Generate income based on age group with normal distribution
            min_inc, max_inc = INCOME_RANGES[age_group]
            income = random.normalvariate((min_inc + max_inc) / 2, (max_inc - min_inc) / 4)
            income = max(min_inc, min(max_inc, income))  # Clip to range
            
            # Create customer profile
            customer = {
                'customer_id': f'CUST{i+1:06d}',
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'phone': self.fake.phone_number(),
                'address': self.fake.street_address(),
                'city': self.fake.city(),
                'state': self.fake.state_abbr(),
                'zip_code': self.fake.zipcode(),
                'date_of_birth': birth_date.date(),
                'ssn': self.fake.ssn().replace('-', ''),  # Remove dashes for consistency
                'customer_since': self.fake.date_between(start_date='-10y', end_date='today'),
                'credit_score': max(300, min(850, int(random.normalvariate(700, 100)))),
                'annual_income': int(income),
                'employment_status': random.choices(
                    ['Employed', 'Self-Employed', 'Unemployed', 'Retired'],
                    weights=[0.6, 0.15, 0.1, 0.15]  # Realistic employment distribution
                )[0],
                'branch_id': random.choice(branch_df['branch_id'].tolist())
            }
            customers.append(customer)
        
        return pd.DataFrame(customers)

    # ============================================================================
    # ACCOUNT DATA GENERATION
    # ============================================================================
    
    def generate_accounts(self, customer_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate bank accounts with realistic types, balances, and relationships.
        
        Each customer can have multiple accounts of different types (Checking, Savings, etc.)
        with type-appropriate balances and interest rates. Accounts have realistic statuses.
        
        Args:
            customer_df (pd.DataFrame): Customers data for account-customer relationships
            
        Returns:
            pd.DataFrame: Accounts data with financial attributes and status
        """
        accounts = []
        account_counter = 1
        
        for _, customer in customer_df.iterrows():
            # Customers have 1-4 accounts on average (exponential distribution)
            num_accounts = max(1, int(random.expovariate(1/1.5)))
            
            for _ in range(num_accounts):
                # Account type distribution (Checking most common)
                account_type = random.choices(
                    ['Checking', 'Savings', 'Money Market', 'CD'],  # Certificate of Deposit
                    weights=[0.5, 0.3, 0.15, 0.05]  # Realistic distribution
                )[0]
                
                # Type-appropriate balance ranges (normal distribution)
                balance_ranges = {
                    'Checking': (0, 5000, 3000),    # (min, mean, std)
                    'Savings': (0, 15000, 10000),
                    'Money Market': (0, 25000, 15000),
                    'CD': (1000, 10000, 5000)
                }
                
                min_bal, mean_bal, std_bal = balance_ranges[account_type]
                balance = max(min_bal, random.normalvariate(mean_bal, std_bal))
                
                # Convert customer_since to date object if needed
                customer_since = self._ensure_date(customer['customer_since'])
                
                account = {
                    'account_id': f'ACC{account_counter:06d}',
                    'customer_id': customer['customer_id'],
                    'account_type': account_type,
                    'account_number': ''.join([str(random.randint(0, 9)) for _ in range(12)]),
                    'current_balance': round(balance, 2),
                    'open_date': self.fake.date_between(start_date=customer_since, end_date='today'),
                    'interest_rate': self._get_interest_rate(account_type),
                    'status': random.choices(['Active', 'Dormant', 'Closed'], 
                                           weights=[0.85, 0.1, 0.05])[0]  # Status distribution
                }
                accounts.append(account)
                account_counter += 1
        
        return pd.DataFrame(accounts)
    
    def _get_interest_rate(self, account_type: str) -> float:
        """Get realistic interest rate based on account type."""
        rates = {
            'Checking': 0.0001,    # Near zero
            'Savings': (0.01, 0.03),
            'Money Market': (0.02, 0.04),
            'CD': (0.025, 0.05)
        }
        
        if account_type == 'Checking':
            return rates['Checking']
        else:
            min_rate, max_rate = rates[account_type]
            return round(random.uniform(min_rate, max_rate), 4)

    # ============================================================================
    # TRANSACTION DATA GENERATION
    # ============================================================================
    
    def generate_transactions(self, account_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate realistic transaction patterns with temporal and behavioral characteristics.
        
        Creates transaction history with:
        - Time-of-day patterns (peak hours, overnight lows)
        - Day-of-week patterns (weekend vs weekday spending)
        - Account-type specific behavior (checking vs savings)
        - Realistic amount distributions
        - Merchant categorization
        
        Args:
            account_df (pd.DataFrame): Accounts data for transaction-account relationships
            
        Returns:
            pd.DataFrame: Transactions data with temporal and financial attributes
        """
        transactions = []
        transaction_id = 1
        
        # Hourly transaction pattern (24-hour distribution)
        HOURLY_PATTERN = [
            0.01, 0.005, 0.002, 0.001, 0.001, 0.005, 0.02, 0.05,  # Overnight to morning
            0.07, 0.06, 0.05, 0.06, 0.07, 0.06, 0.05, 0.06,       # Daytime
            0.07, 0.08, 0.06, 0.04, 0.03, 0.02, 0.01, 0.005        # Evening to night
        ]
        
        for _, account in account_df.iterrows():
            if account['status'] != 'Active':
                continue  # Skip inactive accounts
                
            # Calculate account activity period
            account_open_date = self._ensure_date(account['open_date'])
            days_active = (self.end_date.date() - account_open_date).days
            
            if days_active <= 0:
                continue  # Account opened after our end date
                
            # Transaction frequency based on account type
            txn_frequency = self._get_transaction_frequency(account['account_type'])
            total_expected_txns = int(days_active * txn_frequency)
            actual_txns = max(1, int(random.normalvariate(total_expected_txns, total_expected_txns * 0.1)))
            
            # Simulate transaction history with running balance
            current_balance = account['current_balance']
            running_balance = current_balance
            
            for _ in range(actual_txns):
                # Random transaction date within account lifetime
                days_ago = random.randint(0, days_active)
                transaction_date = self._create_transaction_datetime(account_open_date, days_ago, HOURLY_PATTERN)
                
                # Generate transaction details
                txn_details = self._generate_transaction_details(account['account_type'])
                amount = txn_details['amount']
                
                # Balance management with overdraft limits
                if running_balance + amount < -1000:  # Reasonable overdraft limit
                    amount = -running_balance + random.uniform(0, 100)  # Partial payment
                
                running_balance += amount
                
                transaction = {
                    'transaction_id': f'TXN{transaction_id:08d}',
                    'account_id': account['account_id'],
                    'transaction_date': transaction_date,
                    'transaction_type': txn_details['type'],
                    'amount': round(amount, 2),
                    'balance_after': round(running_balance, 2),
                    'merchant_name': txn_details['merchant'],
                    'merchant_category': txn_details['category'],
                    'description': txn_details['description'],
                    'status': 'Completed' if random.random() > 0.01 else 'Failed'  # 1% failure rate
                }
                
                transactions.append(transaction)
                transaction_id += 1
        
        return pd.DataFrame(transactions)
    
    def _get_transaction_frequency(self, account_type: str) -> float:
        """Get average daily transactions based on account type."""
        frequencies = {
            'Checking': (2, 1),   # (mean, std) - Higher frequency
            'Savings': (0.3, 0.2),
            'Money Market': (0.2, 0.15),
            'CD': (0.05, 0.03)    # Very low frequency
        }
        mean, std = frequencies[account_type]
        return max(0.01, random.normalvariate(mean, std))
    
    def _create_transaction_datetime(self, base_date: datetime, days_ago: int, hourly_pattern: List[float]) -> datetime:
        """Create realistic transaction datetime with temporal patterns."""
        transaction_date = datetime.combine(base_date + timedelta(days=days_ago), datetime.min.time())
        hour = random.choices(range(24), weights=hourly_pattern)[0]
        return transaction_date.replace(
            hour=hour, 
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )
    
    def _generate_transaction_details(self, account_type: str) -> Dict:
        """Generate realistic transaction details based on account type."""
        if account_type == 'Checking':
            # Checking accounts have diverse transaction types
            txn_types = ['POS', 'ATM', 'Transfer', 'Online Payment', 'Direct Deposit']
            weights = [0.4, 0.2, 0.15, 0.2, 0.05]
        else:
            # Savings-like accounts have simpler transaction patterns
            txn_types = ['Deposit', 'Withdrawal', 'Interest', 'Transfer']
            weights = [0.4, 0.3, 0.2, 0.1]
        
        txn_type = random.choices(txn_types, weights=weights)[0]
        
        # Generate type-appropriate amount
        amount = self._generate_transaction_amount(txn_type, account_type)
        
        # Generate merchant details for POS transactions
        merchant, category, description = self._generate_merchant_details(txn_type)
        
        return {
            'type': txn_type,
            'amount': amount,
            'merchant': merchant,
            'category': category,
            'description': description
        }
    
    def _generate_transaction_amount(self, txn_type: str, account_type: str) -> float:
        """Generate realistic transaction amounts based on type."""
        if txn_type in ['Direct Deposit', 'Deposit']:
            return abs(random.normalvariate(1500, 1000))  # Larger deposits
        elif txn_type == 'POS':
            return -abs(random.lognormvariate(3.5, 1.2))  # Log-normal for spending
        elif txn_type == 'ATM':
            return -random.choice([20, 40, 60, 80, 100])  # Typical ATM withdrawals
        elif txn_type == 'Interest':
            return abs(random.normalvariate(50, 20))      # Interest payments
        else:
            return random.normalvariate(0, 500) * random.choice([-1, 1])  # Transfers
    
    def _generate_merchant_details(self, txn_type: str) -> Tuple[Optional[str], Optional[str], str]:
        """Generate merchant information for transaction descriptions."""
        if txn_type == 'POS':
            categories = ['Retail', 'Groceries', 'Dining', 'Utilities', 'Entertainment', 
                         'Travel', 'Healthcare', 'Education', 'Other']
            return (
                self.fake.company(),
                random.choice(categories),
                self.fake.sentence(nb_words=6)
            )
        elif txn_type == 'ATM':
            return (None, None, 'ATM Withdrawal')
        elif txn_type == 'Interest':
            return (None, None, 'Interest Payment')
        else:
            return (None, None, self.fake.sentence(nb_words=6))

    # ============================================================================
    # LOAN DATA GENERATION
    # ============================================================================
    
    def generate_loans(self, customer_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate loan data with realistic terms, payments, and statuses.
        
        Creates various loan types (Mortgage, Auto, Personal, Student) with 
        appropriate amounts, terms, and interest rates. Includes payment calculations.
        
        Args:
            customer_df (pd.DataFrame): Customers data for loan-customer relationships
            
        Returns:
            pd.DataFrame: Loans data with financial terms and status
        """
        loans = []
        
        # 30% of customers have loans (realistic penetration)
        loan_customers = customer_df.sample(frac=0.3)
        
        for i, (_, customer) in enumerate(loan_customers.iterrows()):
            loan_type = random.choices(
                ['Mortgage', 'Auto', 'Personal', 'Student'],
                weights=[0.4, 0.3, 0.2, 0.1]  # Mortgage most common
            )[0]
            
            # Loan parameters based on type
            loan_terms = self._get_loan_parameters(loan_type)
            amount = loan_terms['amount']
            term = loan_terms['term']
            interest_rate = loan_terms['rate']
            
            # Calculate monthly payment using amortization formula
            monthly_rate = interest_rate / 12
            monthly_payment = amount * (monthly_rate * (1 + monthly_rate) ** term) / ((1 + monthly_rate) ** term - 1)
            
            # Loan dates relative to customer relationship
            customer_since = self._ensure_date(customer['customer_since'])
            loan_date = self.fake.date_between(start_date=customer_since, end_date='today')
            
            loans.append({
                'loan_id': f'LOAN{i+1:06d}',
                'customer_id': customer['customer_id'],
                'loan_type': loan_type,
                'loan_amount': amount,
                'interest_rate': round(interest_rate, 4),
                'term_months': term,
                'start_date': loan_date,
                'monthly_payment': round(monthly_payment, 2),
                'remaining_balance': round(amount * random.uniform(0.1, 0.9), 2),  # Some paid off
                'status': random.choices(['Current', 'Delinquent', 'Paid Off'], 
                                       weights=[0.85, 0.1, 0.05])[0]  # Status distribution
            })
        
        return pd.DataFrame(loans)
    
    def _get_loan_parameters(self, loan_type: str) -> Dict:
        """Get realistic loan parameters based on loan type."""
        parameters = {
            'Mortgage': {
                'amount': random.randint(100_000, 500_000),  # $100K-$500K
                'term': random.choice([180, 240, 360]),      # 15, 20, 30 years
                'rate': random.uniform(0.03, 0.06)           # 3-6%
            },
            'Auto': {
                'amount': random.randint(10_000, 50_000),    # $10K-$50K
                'term': random.choice([36, 48, 60, 72]),     # 3-6 years
                'rate': random.uniform(0.04, 0.08)           # 4-8%
            },
            'Personal': {
                'amount': random.randint(5_000, 50_000),     # $5K-$50K
                'term': random.choice([12, 24, 36, 48, 60]), # 1-5 years
                'rate': random.uniform(0.06, 0.12)           # 6-12%
            },
            'Student': {
                'amount': random.randint(10_000, 100_000),   # $10K-$100K
                'term': random.choice([120, 180, 240]),      # 10-20 years
                'rate': random.uniform(0.04, 0.08)           # 4-8%
            }
        }
        return parameters[loan_type]

    # ============================================================================
    # CREDIT CARD DATA GENERATION
    # ============================================================================
    
    def generate_credit_cards(self, customer_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate credit card data with realistic limits, balances, and terms.
        
        Creates credit card accounts with type-appropriate limits, current balances,
        and card network affiliations. Includes expiration dates and status tracking.
        
        Args:
            customer_df (pd.DataFrame): Customers data for card-customer relationships
            
        Returns:
            pd.DataFrame: Credit cards data with financial attributes
        """
        cards = []
        
        # 60% of customers have credit cards (realistic penetration)
        card_customers = customer_df.sample(frac=0.6)
        
        for i, (_, customer) in enumerate(card_customers.iterrows()):
            # Realistic credit limit distribution
            credit_limit = max(1000, min(50_000, int(random.normalvariate(8000, 4000))))
            current_balance = random.randint(0, int(credit_limit * 0.8))  # 0-80% utilization
            
            customer_since = self._ensure_date(customer['customer_since'])
            
            cards.append({
                'card_id': f'CARD{i+1:06d}',
                'customer_id': customer['customer_id'],
                'card_number': ''.join([str(random.randint(0, 9)) for _ in range(16)]),
                'expiry_date': self.fake.date_between(start_date='today', end_date='+5y'),
                'credit_limit': credit_limit,
                'current_balance': current_balance,
                'available_credit': credit_limit - current_balance,
                'issue_date': self.fake.date_between(start_date=customer_since, end_date='today'),
                'card_type': random.choice(['Visa', 'MasterCard', 'American Express']),
                'status': random.choices(['Active', 'Inactive', 'Blocked'], 
                                       weights=[0.9, 0.08, 0.02])[0]  # Status distribution
            })
        
        return pd.DataFrame(cards)

    # ============================================================================
    # UTILITY METHODS
    # ============================================================================
    
    def _ensure_date(self, date_value) -> datetime.date:
        """Convert string date to date object if necessary."""
        if isinstance(date_value, str):
            return datetime.strptime(date_value, '%Y-%m-%d').date()
        return date_value


def generate_complete_dataset() -> Dict[str, pd.DataFrame]:
    """
    Generate complete banking dataset with all interconnected tables.
    
    Orchestrates the generation process in proper dependency order:
    1. Branches (independent)
    2. Customers (depends on branches)
    3. Accounts (depends on customers)
    4. Transactions (depends on accounts)
    5. Loans (depends on customers)
    6. Credit Cards (depends on customers)
    
    Returns:
        Dict[str, pd.DataFrame]: Dictionary of DataFrames for each table
    """
    generator = BankingDataGenerator(customer_count=10000, transaction_years=5)
    
    print("🚀 Starting FinBank Synthetic Data Generation...")
    print("=" * 60)
    
    print("📍 Generating branches...")
    branches = generator.generate_branches()
    
    print("👥 Generating customers...")
    customers = generator.generate_customers(branches)
    
    print("💳 Generating accounts...")
    accounts = generator.generate_accounts(customers)
    
    print("💰 Generating transactions...")
    transactions = generator.generate_transactions(accounts)
    
    print("🏠 Generating loans...")
    loans = generator.generate_loans(customers)
    
    print("💳 Generating credit cards...")
    credit_cards = generator.generate_credit_cards(customers)
    
    print("✅ Data generation completed successfully!")
    print("=" * 60)
    
    return {
        'branches': branches,
        'customers': customers,
        'accounts': accounts,
        'transactions': transactions,
        'loans': loans,
        'credit_cards': credit_cards
    }


def save_datasets(data_dict: Dict[str, pd.DataFrame], output_path: str = 'banking_data/') -> None:
    """
    Save all generated datasets to CSV files for persistent storage.
    
    Args:
        data_dict (Dict[str, pd.DataFrame]): Dictionary of DataFrames to save
        output_path (str): Directory path for saving CSV files
    """
    os.makedirs(output_path, exist_ok=True)
    
    print(f"💾 Saving datasets to {output_path}...")
    for name, df in data_dict.items():
        filename = f"{output_path}/{name}.csv"
        df.to_csv(filename, index=False)
        print(f"   📄 {filename} - {len(df):,} records")
    
    print("✅ All datasets saved successfully!")


def generate_dataset_summary(data_dict: Dict[str, pd.DataFrame]) -> None:
    """
    Generate and display comprehensive summary of the created dataset.
    
    Args:
        data_dict (Dict[str, pd.DataFrame]): Dictionary of DataFrames to summarize
    """
    print("\n" + "=" * 60)
    print("📊 FINBANK DATASET SUMMARY")
    print("=" * 60)
    
    # Basic record counts
    for name, df in data_dict.items():
        print(f"   {name:12} : {len(df):>8,} records")
    
    # Transaction timeframe
    if 'transactions' in data_dict:
        txn_df = data_dict['transactions']
        min_date = txn_df['transaction_date'].min()
        max_date = txn_df['transaction_date'].max()
        print(f"\n   📅 Transaction Period: {min_date} to {max_date}")
        print(f"   💰 Total Transactions: {len(txn_df):,}")
    
    # Financial overview
    if 'accounts' in data_dict:
        total_deposits = data_dict['accounts']['current_balance'].sum()
        print(f"   🏦 Total Deposits: ${total_deposits:,.2f}")
    
    if 'loans' in data_dict:
        total_loans = data_dict['loans']['loan_amount'].sum()
        print(f"   🏠 Total Loan Portfolio: ${total_loans:,.2f}")
    
    print("=" * 60)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    """
    Main execution block for standalone dataset generation.
    
    When run as a script, this generates the complete dataset, saves it to CSV files,
    and displays a comprehensive summary.
    """
    try:
        # Generate complete dataset
        banking_data = generate_complete_dataset()
        
        # Save to CSV files
        save_datasets(banking_data, 'finbank_data/')
        
        # Display summary
        generate_dataset_summary(banking_data)
        
        print("\n🎉 FinBank Synthetic Dataset Generation Completed!")
        print("   Your dataset is ready for data warehousing and analysis!")
        
    except Exception as e:
        print(f"❌ Error during data generation: {e}")
        raise
