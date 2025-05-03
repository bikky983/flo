import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import sys
from datetime import datetime, timedelta


class FloorsheetSummarizer:
    def __init__(self, input_file="public/date_summarized_floorsheet.parquet", 
                 output_file="public/summarized_floorsheet.parquet",
                 retention_days=365):
        """
        Initialize the summarizer for floorsheet data
        
        Args:
            input_file: Path to input date-wise summarized data file
            output_file: Path to output combined summarized data file
            retention_days: Number of days to retain data (default 365 days)
        """
        self.input_file = input_file
        self.output_file = output_file
        self.retention_days = retention_days
        self.cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime("%Y-%m-%d")
    
    def load_data(self):
        """
        Load all date-wise summarized data from parquet file without filtering
        """
        try:
            if not os.path.exists(self.input_file):
                print(f"Input file not found: {self.input_file}")
                return pd.DataFrame()
            
            # Load all date-wise summarized data without applying time filtering
            # since date_summarized_floorsheet.parquet already has 1-year retention policy
            df = pd.read_parquet(self.input_file)
            print(f"Loaded {len(df)} total records from {self.input_file}")
            print(f"Found data for {len(df['date'].unique())} dates")
            
            return df
        except Exception as e:
            print(f"Error loading data: {e}")
            return pd.DataFrame()
    
    def aggregate_broker_stock_data(self, df):
        """
        Combine all date-wise data into a single aggregated summary
        
        Args:
            df: pandas.DataFrame with date-wise summarized data
        
        Returns:
            pandas.DataFrame: Aggregated data across all dates
        """
        if df.empty:
            print("No data to aggregate.")
            return pd.DataFrame()
        
        try:
            print(f"Aggregating data from {len(df['date'].unique())} dates")
            
            # Create broker-stock level aggregations
            broker_stock_aggs = {}
            
            # Process each row in the date-wise summary
            for _, row in df.iterrows():
                key = (row['broker_id'], row['broker_name'], row['symbol'])
                
                if key not in broker_stock_aggs:
                    # Initialize new record
                    broker_stock_aggs[key] = {
                        'broker_id': row['broker_id'],
                        'broker_name': row['broker_name'],
                        'symbol': row['symbol'],
                        'buy_quantity': row['buy_quantity'],
                        'buy_amount': row['buy_amount'],
                        'sell_quantity': row['sell_quantity'],
                        'sell_amount': row['sell_amount'],
                        'last_updated': row['date']  # Use the date as last_updated
                    }
                else:
                    # Update existing record
                    broker_stock_aggs[key]['buy_quantity'] += row['buy_quantity']
                    broker_stock_aggs[key]['buy_amount'] += row['buy_amount']
                    broker_stock_aggs[key]['sell_quantity'] += row['sell_quantity']
                    broker_stock_aggs[key]['sell_amount'] += row['sell_amount']
                    
                    # Update last_updated to most recent date
                    if row['date'] > broker_stock_aggs[key]['last_updated']:
                        broker_stock_aggs[key]['last_updated'] = row['date']
            
            # Convert to DataFrame
            agg_df = pd.DataFrame(list(broker_stock_aggs.values()))
            
            # Calculate derived metrics if we have data
            if not agg_df.empty:
                # Calculate average buy price (handle division by zero)
                agg_df['avg_buy_price'] = agg_df.apply(
                    lambda x: x['buy_amount'] / x['buy_quantity'] if x['buy_quantity'] > 0 else 0, 
                    axis=1
                )
                
                # Calculate average sell price (handle division by zero)
                agg_df['avg_sell_price'] = agg_df.apply(
                    lambda x: x['sell_amount'] / x['sell_quantity'] if x['sell_quantity'] > 0 else 0, 
                    axis=1
                )
                
                # Calculate net position (buy quantity - sell quantity)
                agg_df['net_quantity'] = agg_df['buy_quantity'] - agg_df['sell_quantity']
                
                # Calculate average holding price for net position
                agg_df['avg_holding_price'] = agg_df.apply(
                    lambda x: (x['buy_amount'] - x['sell_amount']) / x['net_quantity'] 
                    if x['net_quantity'] > 0 else 0,
                    axis=1
                )
                
                print(f"Created aggregated summary with {len(agg_df)} broker-stock combinations")
            
            return agg_df
        except Exception as e:
            print(f"Error aggregating data: {e}")
            return pd.DataFrame()
    
    def save_aggregated_data(self, df):
        """
        Save the aggregated DataFrame to a Parquet file, always overwriting previous data
        
        Args:
            df: pandas.DataFrame to save
        
        Returns:
            bool: Success status
        """
        if df.empty:
            print("No data to save.")
            return False
        
        try:
            # Ensure the output directory exists
            output_dir = os.path.dirname(self.output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                print(f"Created output directory: {output_dir}")
            
            # Save the aggregated data (overwrite existing file)
            print(f"Saving aggregated data with {len(df)} records to {self.output_file}")
            table = pa.Table.from_pandas(df)
            pq.write_table(table, self.output_file)
            
            print(f"Successfully saved {len(df)} records to {self.output_file}")
            return True
        except Exception as e:
            print(f"Error saving aggregated data: {e}")
            return False
    
    def run(self):
        """Run the entire data aggregation process"""
        # Load all date-wise summarized data
        date_summarized_data = self.load_data()
        if date_summarized_data.empty:
            print("No date-summarized data to aggregate.")
            return False
        
        # Create overall aggregation
        aggregated_data = self.aggregate_broker_stock_data(date_summarized_data)
        if aggregated_data.empty:
            print("Failed to create aggregated data.")
            return False
        
        # Save aggregated data (overwriting previous file)
        success = self.save_aggregated_data(aggregated_data)
        
        return success


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Aggregate date-wise floorsheet data into a single summary')
    parser.add_argument('--input', type=str, default='public/date_summarized_floorsheet.parquet',
                      help='Input file path for date-wise summarized data')
    parser.add_argument('--output', type=str, default='public/summarized_floorsheet.parquet',
                      help='Output file path for aggregated data')
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
    
    print(f"Using date-summarized data with {args.retention_days}-day retention policy")
    
    # Create summarizer instance
    summarizer = FloorsheetSummarizer(
        input_file=args.input, 
        output_file=args.output,
        retention_days=args.retention_days
    )
    
    # Run the summarization process
    success = summarizer.run()
    
    if success:
        print("\nData aggregation completed successfully.")
        print(f"Aggregated data saved to: {args.output}")
        print(f"This file contains aggregated data for all dates in the input file")
    else:
        print("\nData aggregation failed.")
        sys.exit(1)


if __name__ == "__main__":
    main() 