import pandas as pd
import logging
from sqlalchemy import create_engine
import chrono
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_data(file_path):
    """Extract data from CSV file using loadFileData."""
    try:
        logger.info(f"Extracting data from {file_path}")
        csv_data = loadFileData(file_path)
        df = pd.read_csv(pd.io.common.StringIO(csv_data))
        logger.info(f"Extracted {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise

def transform_data(df):
    """Transform the extracted data."""
    try:
        logger.info("Transforming data")
        # Clean data
        df['ms_played'] = df['ms_played'].fillna(0).astype(int)
        df['ts'] = df['ts'].apply(lambda x: chrono.parseDate(x) if pd.notnull(x) else None)
        df['track_name'] = df['track_name'].str.strip().replace('', None)
        df['artist_name'] = df['artist_name'].str.strip().replace('', None)
        df['album_name'] = df['album_name'].str.strip().replace('', None)
        df['shuffle'] = df['shuffle'].apply(lambda x: x.strip().upper() == 'TRUE' if pd.notnull(x) else False)
        df['skipped'] = df['skipped'].apply(lambda x: x.strip().upper() == 'TRUE' if pd.notnull(x) else False)
        
        # Drop rows with null timestamps or track names
        df = df.dropna(subset=['ts', 'track_name'])
        
        # Aggregations
        artist_summary = df.groupby('artist_name').agg({
            'ms_played': 'sum',
            'track_name': 'count'
        }).rename(columns={'track_name': 'play_count'}).reset_index()
        artist_summary['ms_played'] = artist_summary['ms_played'].astype(int)
        
        # Platform usage
        platform_summary = df.groupby('platform')['ms_played'].sum().reset_index()
        
        # Daily listening trends
        df['date'] = df['ts'].dt.date
        daily_trends = df.groupby('date')['ms_played'].sum().reset_index()
        
        # Interesting insight: Most skipped artist
        skipped_tracks = df[df['skipped'] == True]
        most_skipped_artist = skipped_tracks.groupby('artist_name')['track_name'].count().idxmax() if not skipped_tracks.empty else None
        
        logger.info(f"Transformed data: {len(artist_summary)} artists, {len(platform_summary)} platforms, {len(daily_trends)} days")
        return {
            'listening_history': df,
            'artist_summary': artist_summary,
            'platform_summary': platform_summary,
            'daily_trends': daily_trends,
            'most_skipped_artist': most_skipped_artist
        }
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def load_data(data_dict, db_connection_string):
    """Load transformed data into PostgreSQL."""
    try:
        logger.info("Loading data to PostgreSQL")
        engine = create_engine(db_connection_string)
        data_dict['listening_history'].to_sql('listening_history', engine, if_exists='replace', index=False)
        data_dict['artist_summary'].to_sql('artist_summary', engine, if_exists='replace', index=False)
        data_dict['platform_summary'].to_sql('platform_summary', engine, if_exists='replace', index=False)
        data_dict['daily_trends'].to_sql('daily_trends', engine, if_exists='replace', index=False)
        logger.info("Data loaded successfully")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def generate_report_data(data_dict, output_path):
    """Generate JSON data for the report."""
    try:
        logger.info("Generating report data")
        report_data = {
            'top_artists': data_dict['artist_summary'].nlargest(5, 'ms_played').to_dict(orient='records'),
            'platform_summary': data_dict['platform_summary'].to_dict(orient='records'),
            'daily_trends': data_dict['daily_trends'].to_dict(orient='records'),
            'most_skipped_artist': data_dict['most_skipped_artist']
        }
        with open(output_path, 'w') as f:
            json.dump(report_data, f)
        logger.info(f"Report data saved to {output_path}")
    except Exception as e:
        logger.error(f"Error generating report data: {e}")
        raise

def run_etl():
    """Run the full ETL pipeline."""
    file_path = '/opt/airflow/data/spotify_history.csv'
    db_connection_string = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
    report_data_path = '/opt/airflow/reports/spotify_report_data.json'
    
    df = extract_data(file_path)
    data_dict = transform_data(df)
    load_data(data_dict, db_connection_string)
    generate_report_data(data_dict, report_data_path)
    logger.info("ETL pipeline completed successfully")