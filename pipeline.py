# pipeline.py
# Stack Overflow Tag Analysis — ETL Pipeline
# Author: [Your Name]

import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import os

PROJECT_ID = 'YOUR_PROJECT_ID_HERE'  # Replace with your GCP project ID
OUTPUT_DIR = 'output'

START_DATE = '2020-01-01'
END_DATE = '2024-01-01'
MIN_QUESTIONS = 500

# ─────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────
def extract(client: bigquery.Client) -> pd.DataFrame:
    """Pulls raw tag statistics from BigQuery."""
    print('[EXTRACT] Running BigQuery query...')

    query = f'''
    SELECT tag, COUNT(*) AS total_questions,
        ROUND(AVG(score), 2) AS avg_score,
        ROUND(AVG(view_count), 0) AS avg_views,
        COUNTIF(answer_count = 0) AS unanswered_count,
        ROUND(AVG(answer_count), 2) AS avg_answers
    FROM `bigquery-public-data.stackoverflow.posts_questions`,
        UNNEST(SPLIT(tags, '|')) AS tag
    WHERE creation_date >= '{START_DATE}'
        AND creation_date < '{END_DATE}'
    GROUP BY tag
    HAVING COUNT(*) > {MIN_QUESTIONS}
    ORDER BY total_questions DESC
    LIMIT 100
    '''

    # YOUR TASK: run the query and return a DataFrame
    # Hint: client.query(query).to_dataframe()
    df = None  # Replace this line

    print(f'[EXTRACT] Retrieved {len(df)} rows from BigQuery.')
    return df


# ─────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and enriches the raw data."""
    print('[TRANSFORM] Cleaning and enriching data...')

    # Step 1: Drop nulls in 'tag' and 'total_questions'
    pass

    # Step 2: Remove tags shorter than 2 characters (noise)
    pass

    # Step 3: Create 'unanswered_rate' = unanswered_count / total_questions
    pass

    # Step 4: Create 'engagement_score'
    # Formula: (avg_score * 0.4) + (avg_views / 1000 * 0.4) + (avg_answers * 0.2)
    pass

    # Step 5: Sort by engagement_score descending, reset index
    pass

    print(f'[TRANSFORM] Done. {len(df)} tags remaining.')
    return df


# ─────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────
def load(df: pd.DataFrame) -> None:
    """Saves transformed data to a timestamped CSV."""
    print('[LOAD] Saving results...')

    # Step 1: Create output directory if it doesn't exist
    pass

    # Step 2: Build timestamped filename
    # e.g. output/stackoverflow_tags_20241015_143022.csv
    pass

    # Step 3: Save DataFrame to CSV
    pass
    print(f'[LOAD] Saved to {filename}')

    print('[LOAD] Pipeline complete!')


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    print('=' * 50)
    print(' Stack Overflow Tag Analysis — ETL Pipeline')
    print('=' * 50)

    client = bigquery.Client(project=PROJECT_ID)

    raw_df = extract(client)
    transformed_df = transform(raw_df)
    load(transformed_df)

    print('Pipeline finished successfully.')


if __name__ == '__main__':
    main()