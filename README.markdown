# Spotify Data Engineering Project

This project implements an ETL pipeline to process Spotify streaming history data using Apache Airflow, Python, and PostgreSQL. It also generates an interactive web-based report.

## Prerequisites
- Docker
- Docker Compose

## Setup Instructions
1. Clone the repository:
   ```bash
   git clone <repository_url>
   cd spotify_data_project
   ```

2. Create the directory structure:
   ```bash
   mkdir -p dags data scripts reports
   ```

3. Place `spotify_history.csv` in `data/`, `spotify_etl_pipeline.py` in `dags/`, `spotify_etl.py` in `scripts/`, `spotify_report.html` in `reports/`, and `docker-compose.yml`, `Dockerfile`, `requirements.txt` in the root.

4. Build and start the Docker containers:
   ```bash
   docker-compose up --build
   ```

5. Access the Airflow UI at `http://localhost:8080` (default credentials: `airflow`/`airflow`).

6. Enable the `spotify_etl_pipeline` DAG in the Airflow UI to run the ETL process.

7. View the report at `http://localhost`.

## Project Structure
- `dags/`: Airflow DAG definitions.
- `data/`: Input CSV files.
- `scripts/`: ETL logic.
- `reports/`: Web-based report.
- `docker-compose.yml`: Docker services.
- `Dockerfile`: Custom Airflow image.
- `requirements.txt`: Python dependencies.

## Viewing Results
- **Database**: Connect to PostgreSQL at `localhost:5432` with credentials `airflow`/`airflow`. Query tables:
  ```sql
  SELECT * FROM listening_history;
  SELECT * FROM artist_summary;
  SELECT * FROM platform_summary;
  SELECT * FROM daily_trends;
  ```
- **Report**: Open `http://localhost` to view the interactive report.

## Notes
- The report includes visualizations of top artists, platform usage, and daily listening trends.
- The most skipped artist is highlighted as an interesting insight.
- For production, update credentials and consider using `CeleryExecutor` for Airflow.