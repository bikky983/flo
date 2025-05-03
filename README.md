# Merolagani Floorsheet Scraper

This project scrapes floorsheet data from [Merolagani](https://merolagani.com/Floorsheet.aspx), saves it as a Parquet file, and provides summaries of broker and stock activities.

## Features

- Downloads transaction data from Merolagani floorsheet
- Handles pagination automatically
- Checks for duplicate records when adding new data
- Saves data in Parquet format
- Creates date-wise summaries of broker-stock activities
- Aggregates data across all dates for a complete view
- Applies data retention policy (default: 1 year)
- Can run automatically via GitHub Actions

## Project Structure

The project has been divided into three main scripts that form a processing pipeline:

1. **floorsheet_downloader.py**: Downloads the latest data from the website and checks for duplicates
2. **floorsheet_date_summarizer.py**: Creates date-wise summaries of broker-stock activities
3. **floorsheet_summarizer.py**: Aggregates all date-wise summaries into a combined view

This separation allows for more flexibility and clearer organization of tasks.

## Data Flow

The data flows through the system as follows:

1. **Raw Data**: `public/raw_floorsheet.parquet`
   - Contains all individual transactions
   - Downloaded from Merolagani website
   - Each row represents a single stock transaction

2. **Date-wise Summaries**: `public/date_summarized_floorsheet.parquet`
   - Summarizes transactions by date, broker, and stock
   - Contains one record per broker-stock-date combination
   - Maintains separate date sections within the file

3. **Combined Summary**: `public/summarized_floorsheet.parquet`
   - Aggregates all date-wise data into a single view
   - Contains one record per broker-stock combination across all dates
   - Gives a complete picture of trading activity

## How to Use

### Download Raw Data

To download the latest floorsheet data:

```
python floorsheet_downloader.py [--date YYYY-MM-DD] [--max-pages N] [--output PATH] [--retention-days DAYS]
```

Options:
- `--date`: Specific date to download (format: YYYY-MM-DD)
- `--max-pages`: Maximum number of pages to download
- `--output`: Output file path (default: public/raw_floorsheet.parquet)
- `--retention-days`: Number of days to retain data (default: 365)

### Create Date-wise Summaries

To create date-wise summaries from raw data:

```
python floorsheet_date_summarizer.py [--input PATH] [--output PATH] [--retention-days DAYS]
```

Options:
- `--input`: Input file path for raw data (default: public/raw_floorsheet.parquet)
- `--output`: Output file path for date-wise summaries (default: public/date_summarized_floorsheet.parquet)
- `--retention-days`: Number of days to retain data (default: 365)

### Create Combined Summary

To create an aggregated summary from date-wise data:

```
python floorsheet_summarizer.py [--input PATH] [--output PATH] [--retention-days DAYS]
```

Options:
- `--input`: Input file path for date-wise data (default: public/date_summarized_floorsheet.parquet)
- `--output`: Output file path for combined summary (default: public/summarized_floorsheet.parquet)
- `--retention-days`: Number of days to retain data (default: 365)

### Full Pipeline

To run the complete pipeline:

```
python floorsheet_downloader.py
python floorsheet_date_summarizer.py
python floorsheet_summarizer.py
```

## Output Files

The scripts create and maintain three main files:

1. **public/raw_floorsheet.parquet**: Contains all the raw transaction data
2. **public/date_summarized_floorsheet.parquet**: Contains date-wise broker-stock summaries
3. **public/summarized_floorsheet.parquet**: Contains the aggregated broker-stock summary data

All files are updated with each run, avoiding duplicate data.

## Data Retention Policy

All three scripts implement a data retention policy that keeps only the most recent data:

1. **Raw Data Retention**: 
   - The downloader removes raw transaction records older than the specified retention period (default: 365 days)
   - This is based on the transaction date

2. **Date-wise Summary Retention**:
   - The date summarizer removes data for dates older than the retention period
   - This is based on the transaction date

3. **Combined Summary Retention**:
   - The summarizer removes broker-stock records that haven't been updated within the retention period
   - This is based on the 'last_updated' field, which tracks when data for a particular broker-stock combination was last seen

This ensures that the dataset doesn't grow indefinitely and focuses on recent, relevant data.

## Duplicate Handling

- **Raw Data**: The downloader checks for duplicate records based on date and transaction number
- **Date-wise Summary**: Each date's data replaces any existing data for that date
- **Combined Summary**: The most recent broker-stock data is used when duplicates are found

## GitHub Workflow

The project includes a GitHub workflow (`.github/workflows/merolagani_scraper.yml`) that:

1. Runs daily at 6:00 AM UTC (configurable)
2. Can be triggered manually via the Actions tab with customizable parameters
3. Installs all required dependencies
4. Runs all three scripts in sequence
5. Uploads the generated data files as artifacts

When manually triggering the workflow, you can specify:
- A specific date to scrape
- Maximum number of pages to process
- Data retention period in days

## How to Access the Data

After the workflow runs, you can download the scraped data by:

1. Going to the Actions tab in your GitHub repository
2. Opening the latest workflow run
3. Downloading the data artifacts

## Dependencies

- Python 3.10 or higher
- See `requirements.txt` for all package dependencies:
  - requests
  - beautifulsoup4
  - pandas
  - pyarrow 