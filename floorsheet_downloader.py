import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from urllib.parse import urljoin
import argparse
import sys


class FloorsheetDownloader:
    def __init__(self, base_url="https://merolagani.com/Floorsheet.aspx", delay_range=(1, 3), target_date=None, retention_days=365):
        """
        Initialize the scraper for merolagani.com floorsheet
        
        Args:
            base_url: The base URL for the floorsheet page
            delay_range: Range of seconds to delay between requests (min, max)
            target_date: Specific date to scrape in format YYYY-MM-DD (None for latest)
            retention_days: Number of days to retain data (default 365 days)
        """
        self.base_url = base_url
        self.delay_range = delay_range
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.session.headers.update(self.headers)
        self.all_data = []
        self.current_date = None
        self.target_date = target_date
        self.retention_days = retention_days
    
    def _random_delay(self):
        """Add a random delay between requests to be respectful to the server"""
        time.sleep(random.uniform(*self.delay_range))
    
    def _get_page(self, page_num=1):
        """
        Get content from a specific page number
        
        Args:
            page_num: The page number to fetch
        
        Returns:
            BeautifulSoup object of the page content
        """
        params = {'pg': page_num} if page_num > 1 else {}
        
        # Add date parameter if target_date is specified
        if self.target_date:
            # Convert YYYY-MM-DD to YYYY/MM/DD for the URL
            date_parts = self.target_date.split('-')
            if len(date_parts) == 3:
                formatted_date = '/'.join(date_parts)
                params['date'] = formatted_date
                print(f"Fetching data for specific date: {formatted_date}")
        
        try:
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            print(f"Error fetching page {page_num}: {e}")
            return None
    
    def _extract_date(self, soup):
        """Extract the trading date from the page"""
        try:
            date_text = soup.find(text=lambda text: text and "As of" in text)
            if date_text:
                # Extract date in format YYYY/MM/DD
                date_parts = date_text.strip().split("As of ")[-1].split()[0]
                return datetime.strptime(date_parts, "%Y/%m/%d").strftime("%Y-%m-%d")
        except Exception as e:
            print(f"Error extracting date: {e}")
        return None
    
    def _extract_transactions(self, soup):
        """
        Extract transaction data from the floorsheet table
        
        Args:
            soup: BeautifulSoup object of the page content
        
        Returns:
            List of dictionaries containing transaction data
        """
        transactions = []
        
        # Find the table with floorsheet data
        table = soup.find('table', {'class': 'table'})
        if not table:
            return transactions

        # Extract date if not already set
        if not self.current_date:
            self.current_date = self._extract_date(soup)
        
        # Process each row in the table
        for row in table.find_all('tr')[1:]:  # Skip header row
            cols = row.find_all('td')
            if len(cols) >= 7:  # Ensure we have all required columns
                try:
                    # Extract data from each column
                    transaction_no = cols[1].text.strip()
                    
                    # Extract symbol and its full name from the link
                    symbol_cell = cols[2].find('a')
                    symbol = symbol_cell.text.strip() if symbol_cell else ""
                    symbol_full = symbol_cell.get('title', "") if symbol_cell else ""
                    
                    # Extract buyer and seller broker IDs
                    buyer_cell = cols[3].find('a')
                    buyer_id = buyer_cell.text.strip() if buyer_cell else ""
                    buyer_name = buyer_cell.get('title', "") if buyer_cell else ""
                    
                    seller_cell = cols[4].find('a')
                    seller_id = seller_cell.text.strip() if seller_cell else ""
                    seller_name = seller_cell.get('title', "") if seller_cell else ""
                    
                    # Extract numeric data
                    quantity = int(cols[5].text.strip().replace(',', ''))
                    rate = float(cols[6].text.strip().replace(',', ''))
                    amount = float(cols[7].text.strip().replace(',', ''))
                    
                    # Create transaction record
                    transaction = {
                        'date': self.current_date,
                        'transaction_no': transaction_no,
                        'symbol': symbol,
                        'symbol_full': symbol_full,
                        'buyer_id': buyer_id,
                        'buyer_name': buyer_name,
                        'seller_id': seller_id,
                        'seller_name': seller_name,
                        'quantity': quantity,
                        'rate': rate,
                        'amount': amount
                    }
                    
                    transactions.append(transaction)
                except Exception as e:
                    print(f"Error processing row: {e}")
                    continue
        
        return transactions
    
    def _get_total_pages(self, soup):
        """
        Extract the total number of pages from pagination
        
        Args:
            soup: BeautifulSoup object of the page content
        
        Returns:
            int: The total number of pages
        """
        try:
            page_info = soup.find(text=lambda text: text and "Total pages:" in text)
            if page_info:
                pages_text = page_info.strip()
                total_pages = int(pages_text.split("Total pages:")[-1].strip().strip(']').strip())
                return total_pages
        except Exception as e:
            print(f"Error getting total pages: {e}")
        
        # Default to 1 page if we can't determine the total
        return 1
    
    def download_floorsheet(self, max_pages=None):
        """
        Download floorsheet data from all pages
        
        Args:
            max_pages: Maximum number of pages to scrape (None for all pages)
        
        Returns:
            pandas.DataFrame: The scraped data
        """
        # Get first page to determine total pages
        first_page = self._get_page(1)
        if not first_page:
            print("Failed to fetch the first page.")
            return pd.DataFrame()
        
        # Extract total pages and current date
        total_pages = self._get_total_pages(first_page)
        self.current_date = self._extract_date(first_page)
        print(f"Date: {self.current_date}, Total pages: {total_pages}")
        
        # Limit pages if specified
        if max_pages:
            total_pages = min(total_pages, max_pages)
        
        # Process the first page data
        transactions = self._extract_transactions(first_page)
        self.all_data.extend(transactions)
        print(f"Processed page 1/{total_pages}, extracted {len(transactions)} transactions")
        
        # Process remaining pages
        for page_num in range(2, total_pages + 1):
            self._random_delay()
            print(f"Fetching page {page_num}/{total_pages}")
            
            page_soup = self._get_page(page_num)
            if page_soup:
                page_transactions = self._extract_transactions(page_soup)
                self.all_data.extend(page_transactions)
                print(f"Processed page {page_num}/{total_pages}, extracted {len(page_transactions)} transactions")
            else:
                print(f"Failed to fetch page {page_num}")
        
        # Convert to DataFrame
        return pd.DataFrame(self.all_data)
    
    def save_to_parquet(self, df, output_file="public/raw_floorsheet.parquet", append=True):
        """
        Save the DataFrame to a Parquet file with duplicate checking
        and retention policy applied (keeping only last year of data)
        
        Args:
            df: pandas.DataFrame to save
            output_file: Name of the output Parquet file
            append: Whether to append to existing file (if it exists)
        """
        if df.empty:
            print("No data to save.")
            return False
        
        try:
            # Create PyArrow Table from DataFrame
            table = pa.Table.from_pandas(df)
            
            # Ensure the output directory exists
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
                print(f"Created output directory: {output_dir}")
            
            # Calculate the cutoff date for data retention (default: 1 year)
            today = datetime.now()
            cutoff_date = (today - timedelta(days=self.retention_days)).strftime("%Y-%m-%d")
            print(f"Data retention policy: Keeping data from {cutoff_date} onwards")
            
            # If append mode and file exists, check for duplicates and apply retention policy
            if append and os.path.exists(output_file):
                try:
                    # Read existing data
                    existing_df = pd.read_parquet(output_file)
                    print(f"Found existing file with {len(existing_df)} records")
                    
                    # Apply retention policy to existing data
                    if 'date' in existing_df.columns:
                        old_count = len(existing_df)
                        existing_df = existing_df[existing_df['date'] >= cutoff_date]
                        removed_count = old_count - len(existing_df)
                        if removed_count > 0:
                            print(f"Removed {removed_count} records older than {cutoff_date}")
                    
                    # Check for duplicates based on date and transaction_no
                    if 'date' in df.columns and 'transaction_no' in df.columns:
                        # Create keys for comparison
                        df['key'] = df['date'] + '-' + df['transaction_no']
                        existing_df['key'] = existing_df['date'] + '-' + existing_df['transaction_no']
                        
                        # Find new records (not duplicates)
                        new_records = df[~df['key'].isin(existing_df['key'])]
                        new_records = new_records.drop(columns=['key'])
                        existing_df = existing_df.drop(columns=['key'])
                        
                        duplicate_count = len(df) - len(new_records)
                        print(f"Found {duplicate_count} duplicate records")
                        
                        if len(new_records) > 0:
                            print(f"Adding {len(new_records)} new records")
                            
                            # Combine data
                            combined_df = pd.concat([existing_df, new_records], ignore_index=True)
                            
                            # Save combined data
                            print(f"Saving combined data with {len(combined_df)} records")
                            table = pa.Table.from_pandas(combined_df)
                            pq.write_table(table, output_file)
                            
                            print(f"Successfully saved {len(new_records)} new records to {output_file}")
                            return True
                        else:
                            print("No new records to add. File remains unchanged.")
                            
                            # Check if we removed old data and need to update the file
                            if removed_count > 0:
                                print(f"Updating file to remove {removed_count} old records")
                                table = pa.Table.from_pandas(existing_df)
                                pq.write_table(table, output_file)
                                print(f"Successfully updated file with {len(existing_df)} records")
                            
                            return True
                    else:
                        # If no way to identify duplicates, just append all
                        combined_df = pd.concat([existing_df, df], ignore_index=True)
                        print(f"Appended all {len(df)} records (no duplicate checking)")
                        
                        # Save combined data
                        table = pa.Table.from_pandas(combined_df)
                        pq.write_table(table, output_file)
                        
                        print(f"Successfully saved combined data with {len(combined_df)} records to {output_file}")
                        return True
                except Exception as e:
                    print(f"Error checking duplicates: {e}")
                    print("Saving new data only")
            
            # Check if current data needs to be filtered by retention policy
            if 'date' in df.columns:
                old_count = len(df)
                df = df[df['date'] >= cutoff_date]
                removed_count = old_count - len(df)
                if removed_count > 0:
                    print(f"Filtered out {removed_count} records older than {cutoff_date}")
                    if df.empty:
                        print("No data left after applying retention policy")
                        return False
                    table = pa.Table.from_pandas(df)
            
            # Write to Parquet file (new file or error handling case)
            pq.write_table(table, output_file)
            
            print(f"Successfully saved {len(df)} records to {output_file}")
            return True
        except Exception as e:
            print(f"Error saving to Parquet: {e}")
            return False


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Download floorsheet data from merolagani.com')
    parser.add_argument('--date', type=str, help='Specific date to download in format YYYY-MM-DD')
    parser.add_argument('--max-pages', type=int, default=None, help='Maximum number of pages to download')
    parser.add_argument('--output', type=str, default='public/raw_floorsheet.parquet', 
                      help='Output file path for the raw data')
    parser.add_argument('--retention-days', type=int, default=365,
                      help='Number of days to retain data (default: 365)')
    args = parser.parse_args()
    
    # Create downloader instance with target date if specified
    downloader = FloorsheetDownloader(
        target_date=args.date,
        retention_days=args.retention_days
    )
    
    # Ensure output directory exists
    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        print(f"Ensuring output directory exists: {output_dir}")
    
    # Print info about the downloading task
    if args.date:
        print(f"Downloading floorsheet data for date: {args.date}")
    else:
        print("Downloading latest floorsheet data")
    
    print(f"Data retention policy: {args.retention_days} days")
    
    # Download floorsheet pages
    df = downloader.download_floorsheet(max_pages=args.max_pages)
    
    # Save the raw data with duplicate checking and retention policy
    if not df.empty:
        success = downloader.save_to_parquet(df, output_file=args.output)
        
        if success:
            # Print summary
            print("\nDownload Summary:")
            print(f"Total records downloaded: {len(df)}")
            print(f"Trading date: {downloader.current_date}")
            print(f"Raw data saved to: {args.output}")
            print(f"Data retention: Keeping last {args.retention_days} days only")
        else:
            print("Failed to save the downloaded data")
            sys.exit(1)
    else:
        print("No data was downloaded.")
        sys.exit(1)


if __name__ == "__main__":
    main() 