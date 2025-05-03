import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import sys
from datetime import datetime, timedelta


class FloorsheetDateSummarizer:
    def __init__(self, input_file="public/raw_floorsheet.parquet", 
                 output_file="public/date_summarized_floorsheet.parquet",
                 retention_days=365):
        """
        Initialize the date-wise summarizer for floorsheet data
        
        Args:
            input_file: Path to input raw floorsheet data file
            output_file: Path to output date-wise summarized data file
            retention_days: Number of days to retain data (default 365 days)
        """
        self.input_file = input_file
        self.output_file = output_file
        self.retention_days = retention_days
        self.cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime("%Y-%m-%d")
    
    def load_data(self):
        """
        Load the raw floorsheet data from parquet file
        """
        try:
            if not os.path.exists(self.input_file):
                print(f"Input file not found: {self.input_file}")
                return pd.DataFrame()
            
            # Load all data
            df = pd.read_parquet(self.input_file)
            print(f"Loaded {len(df)} total records from {self.input_file}")
            
            # Apply retention policy
            if 'date' in df.columns and not df.empty:
                old_count = len(df)
                df = df[df['date'] >= self.cutoff_date]
                removed_count = old_count - len(df)
                if removed_count > 0:
                    print(f"Filtered out {removed_count} records older than {self.cutoff_date}")
            
            return df
        except Exception as e:
            print(f"Error loading data: {e}")
            return pd.DataFrame()
    
    def summarize_by_date(self, df):
        """
        Create date-wise summaries of floorsheet data
        
        Args:
            df: pandas.DataFrame with transaction data
        
        Returns:
            dict: Dictionary with date as key and summarized DataFrame as value
        """
        if df.empty:
            print("No data to summarize.")
            return {}
        
        try:
            # Create a copy of the dataframe to avoid modifying the original
            data = df.copy()
            
            # Dictionary to store date-wise summaries
            date_summaries = {}
            
            # Get unique dates
            if 'date' not in data.columns:
                print("Error: 'date' column not found in data")
                return {}
            
            dates = data['date'].unique()
            print(f"Found {len(dates)} unique dates in data")
            
            # Process each date separately
            for date in dates:
                print(f"Processing data for date: {date}")
                date_data = data[data['date'] == date]
                
                # Create broker-stock level aggregations
                broker_stock_aggs = {}
                
                # Process buys - group by buyer broker and stock symbol
                buy_data = date_data.groupby(['buyer_id', 'buyer_name', 'symbol']).agg(
                    buy_quantity=('quantity', 'sum'),
                    buy_amount=('amount', 'sum')
                ).reset_index()
                
                # Process sells - group by seller broker and stock symbol
                sell_data = date_data.groupby(['seller_id', 'seller_name', 'symbol']).agg(
                    sell_quantity=('quantity', 'sum'),
                    sell_amount=('amount', 'sum')
                ).reset_index()
                
                # Create a unified broker-stock aggregation
                # First, add all buy data
                for _, row in buy_data.iterrows():
                    key = (row['buyer_id'], row['symbol'])  # Using broker_id and symbol as key
                    if key not in broker_stock_aggs:
                        broker_stock_aggs[key] = {
                            'date': date,
                            'broker_id': row['buyer_id'],
                            'broker_name': row['buyer_name'],
                            'symbol': row['symbol'],
                            'buy_quantity': row['buy_quantity'],
                            'buy_amount': row['buy_amount'],
                            'sell_quantity': 0,
                            'sell_amount': 0
                        }
                    else:
                        broker_stock_aggs[key]['buy_quantity'] += row['buy_quantity']
                        broker_stock_aggs[key]['buy_amount'] += row['buy_amount']
                
                # Then add all sell data
                for _, row in sell_data.iterrows():
                    key = (row['seller_id'], row['symbol'])  # Using broker_id and symbol as key
                    if key not in broker_stock_aggs:
                        broker_stock_aggs[key] = {
                            'date': date,
                            'broker_id': row['seller_id'],
                            'broker_name': row['seller_name'],
                            'symbol': row['symbol'],
                            'buy_quantity': 0,
                            'buy_amount': 0,
                            'sell_quantity': row['sell_quantity'],
                            'sell_amount': row['sell_amount']
                        }
                    else:
                        broker_stock_aggs[key]['sell_quantity'] += row['sell_quantity']
                        broker_stock_aggs[key]['sell_amount'] += row['sell_amount']
                
                # Convert to DataFrame
                date_df = pd.DataFrame(list(broker_stock_aggs.values()))
                
                # Calculate derived metrics if we have data
                if not date_df.empty:
                    # Calculate average buy price (handle division by zero)
                    date_df['avg_buy_price'] = date_df.apply(
                        lambda x: x['buy_amount'] / x['buy_quantity'] if x['buy_quantity'] > 0 else 0, 
                        axis=1
                    )
                    
                    # Calculate average sell price (handle division by zero)
                    date_df['avg_sell_price'] = date_df.apply(
                        lambda x: x['sell_amount'] / x['sell_quantity'] if x['sell_quantity'] > 0 else 0, 
                        axis=1
                    )
                    
                    # Calculate net position (buy quantity - sell quantity)
                    date_df['net_quantity'] = date_df['buy_quantity'] - date_df['sell_quantity']
                    
                    # Calculate average holding price for net position
                    date_df['avg_holding_price'] = date_df.apply(
                        lambda x: (x['buy_amount'] - x['sell_amount']) / x['net_quantity'] 
                        if x['net_quantity'] > 0 else 0,
                        axis=1
                    )
                    
                    # Store in dictionary
                    date_summaries[date] = date_df
                    print(f"Created summary for date {date} with {len(date_df)} broker-stock combinations")
            
            return date_summaries
        except Exception as e:
            print(f"Error summarizing data by date: {e}")
            return {}
    
    def save_date_summaries(self, date_summaries):
        """
        Save date-wise summaries to a parquet file
        
        Args:
            date_summaries: Dictionary with date as key and summarized DataFrame as value
        
        Returns:
            bool: Success status
        """
        if not date_summaries:
            print("No date summaries to save.")
            return False
        
        try:
            # Ensure the output directory exists
            output_dir = os.path.dirname(self.output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                print(f"Created output directory: {output_dir}")
            
            # Initialize combined DataFrame
            all_date_summaries = []
            all_dates = set()
            
            # Check if we need to merge with existing data
            if os.path.exists(self.output_file):
                try:
                    # Read existing summarized data
                    existing_df = pd.read_parquet(self.output_file)
                    print(f"Found existing date-summarized data with {len(existing_df)} records")
                    
                    # Apply retention policy to existing data
                    if 'date' in existing_df.columns:
                        old_count = len(existing_df)
                        existing_df = existing_df[existing_df['date'] >= self.cutoff_date]
                        removed_count = old_count - len(existing_df)
                        if removed_count > 0:
                            print(f"Removed {removed_count} records older than {self.cutoff_date}")
                    
                    # Get existing dates
                    if not existing_df.empty and 'date' in existing_df.columns:
                        existing_dates = set(existing_df['date'].unique())
                        all_dates.update(existing_dates)
                        
                        # For each date in our new summaries
                        for date, date_df in date_summaries.items():
                            if date in existing_dates:
                                # Replace the existing data for this date
                                filtered_existing = existing_df[existing_df['date'] != date]
                                print(f"Replacing data for date {date}")
                                
                                # Combine existing data (excluding this date) with new data for this date
                                if not filtered_existing.empty:
                                    all_date_summaries.append(filtered_existing)
                                all_date_summaries.append(date_df)
                            else:
                                # Just add new data for dates that don't exist
                                all_dates.add(date)
                                all_date_summaries.append(date_df)
                    else:
                        # Just use new summaries if existing data is invalid
                        for date, date_df in date_summaries.items():
                            all_dates.add(date)
                            all_date_summaries.append(date_df)
                except Exception as e:
                    print(f"Error reading existing date summaries: {e}")
                    # Just use new summaries if there's an error
                    for date, date_df in date_summaries.items():
                        all_dates.add(date)
                        all_date_summaries.append(date_df)
            else:
                # No existing file, just use new summaries
                for date, date_df in date_summaries.items():
                    all_dates.add(date)
                    all_date_summaries.append(date_df)
            
            # Combine all date DataFrames
            if all_date_summaries:
                combined_df = pd.concat(all_date_summaries, ignore_index=True)
                print(f"Saving combined date summaries with {len(combined_df)} records for {len(all_dates)} dates")
                
                # Save to parquet
                table = pa.Table.from_pandas(combined_df)
                pq.write_table(table, self.output_file)
                
                print(f"Successfully saved date-wise summaries to {self.output_file}")
                return True
            else:
                print("No data to save after processing.")
                return False
        except Exception as e:
            print(f"Error saving date summaries: {e}")
            return False
    
    def run(self):
        """Run the entire date-wise summarization process"""
        # Load raw data
        raw_data = self.load_data()
        if raw_data.empty:
            print("No raw data to summarize.")
            return False
        
        # Create date-wise summaries
        date_summaries = self.summarize_by_date(raw_data)
        if not date_summaries:
            print("Failed to create date-wise summaries.")
            return False
        
        # Save date-wise summaries
        success = self.save_date_summaries(date_summaries)
        
        return success


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Create date-wise summaries of floorsheet data')
    parser.add_argument('--input', type=str, default='public/raw_floorsheet.parquet',
                      help='Input file path for raw floorsheet data')
    parser.add_argument('--output', type=str, default='public/date_summarized_floorsheet.parquet',
                      help='Output file path for date-wise summarized data')
    parser.add_argument('--retention-days', type=int, default=365,
                      help='Number of days to retain data (default: 365)')
    args = parser.parse_args()
    
    # Ensure input directory exists (although we don't write to it)
    input_dir = os.path.dirname(args.input)
    if input_dir and not os.path.exists(input_dir):
        print(f"Warning: Input directory does not exist: {input_dir}")
    
    # Ensure output directory exists
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        print(f"Created output directory: {output_dir}")
    
    print(f"Data retention policy: {args.retention_days} days")
    
    # Create date summarizer instance
    summarizer = FloorsheetDateSummarizer(
        input_file=args.input, 
        output_file=args.output,
        retention_days=args.retention_days
    )
    
    # Run the date summarization process
    success = summarizer.run()
    
    if success:
        print("\nDate-wise summarization completed successfully.")
        print(f"Date-wise summarized data saved to: {args.output}")
        print(f"Data retention: Keeping data for the last {args.retention_days} days")
    else:
        print("\nDate-wise summarization failed.")
        sys.exit(1)


if __name__ == "__main__":
    main() 
