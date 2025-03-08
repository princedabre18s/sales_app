# Import required libraries
import streamlit as st
import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
import matplotlib.pyplot as plt
import seaborn as sns
import io
import time
import concurrent.futures
import logging
import csv
from sqlalchemy import create_engine, text
import plotly.express as px
import plotly.graph_objects as go
import shutil
from dotenv import load_dotenv
import ollama
from functools import lru_cache
from scipy import stats

# Load environment variables from .env file
load_dotenv()

logging.info(f"DB_HOST: {os.getenv('DB_HOST')}")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set page configuration
st.set_page_config(
    page_title="Sales & Inventory Data Pipeline",
    page_icon="📊",
    layout="wide"
)

# Define constants
TEMP_STORAGE_DIR = "temp_storage"
PROCESSED_DIR = "processed_data"
MASTER_SUMMARY_FILE = "master_summary.xlsx"
DB_RETENTION_DAYS = 30

# Create necessary directories if they don’t exist
for directory in [TEMP_STORAGE_DIR, PROCESSED_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# Database connection function using .env
def get_db_connection():
    """Establish connection to Neon Database using .env variables"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432"),  # Default to 5432 if not set
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        logger.error(f"Connection details: host={os.getenv('DB_HOST')}, db={os.getenv('DB_NAME')}, port={os.getenv('DB_PORT', '5432')}")
        return None

# SQLAlchemy Engine for pandas operations using .env
def get_sqlalchemy_engine():
    """Create SQLAlchemy engine using .env variables"""
    return create_engine(
        f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT", "5432")}/{os.getenv("DB_NAME")}'
    )

# Function to preprocess the uploaded file with Grand Total removal from the last line
def preprocess_data(file_path, selected_date, log_output):
    """Preprocess the uploaded Excel file, removing the last line if it’s 'Grand Total' in the first column"""
    log_output.info(f"Starting preprocessing of file: {os.path.basename(file_path)}")
    
    try:
        # Load the Excel file starting from row 10 (skiprows=9, 0-based index)
        df = pd.read_excel(file_path, skiprows=9)
        log_output.info(f"File loaded. Raw shape: {df.shape}")
        
        # Identify the first column dynamically
        first_col_name = df.columns[0]
        
        # Check if the last row's first column contains 'grand total' (case-insensitive)
        last_row_first_col = str(df.iloc[-1][first_col_name]).strip().lower()
        if 'grand total' in last_row_first_col:
            df = df.iloc[:-1].copy()  # Remove the entire last row
            log_output.info("Removed the last row as it contained 'Grand Total' in the first column")
        else:
            log_output.info("Last row does not contain 'Grand Total' in the first column; proceeding as is")
        
        # Define required columns
        required_cols = ['Brand', 'Category', 'Size', 'MRP', 'Color', 'SalesQty', 'PurchaseQty']
        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            log_output.error(f"Missing required columns: {missing_cols}")
            return None
        
        # Select only the required columns
        df = df[required_cols].copy()
        
        # Standardize text columns to avoid case sensitivity issues
        for col in ['Brand', 'Category', 'Size', 'Color']:
            df[col] = df[col].str.strip().str.lower()
        
        # Log raw totals and non-zero counts
        raw_sales_total = df['SalesQty'].sum()
        raw_purchase_total = df['PurchaseQty'].sum()
        sales_non_zero = (df['SalesQty'].fillna(0) != 0).sum()
        purchase_non_zero = (df['PurchaseQty'].fillna(0) != 0).sum()
        log_output.info(f"Raw totals before cleaning - SalesQty: {raw_sales_total} (non-zero: {sales_non_zero}), PurchaseQty: {raw_purchase_total} (non-zero: {purchase_non_zero})")
        
        # Smart blank handling and numeric conversion
        for col in ['Brand', 'Category', 'Size', 'Color']:
            df[col] = df[col].fillna('unknown')
        df['MRP'] = df['MRP'].fillna(0.0)
        df['SalesQty'] = pd.to_numeric(df['SalesQty'], errors='coerce').fillna(0).astype(int)
        df['PurchaseQty'] = pd.to_numeric(df['PurchaseQty'], errors='coerce').fillna(0).astype(int)
        
        # Log totals after numeric cleaning
        cleaned_sales_total = df['SalesQty'].sum()
        cleaned_purchase_total = df['PurchaseQty'].sum()
        log_output.info(f"Totals after numeric cleaning - SalesQty: {cleaned_sales_total}, PurchaseQty: {cleaned_purchase_total}")
        
        # Ensure date is a Timestamp
        selected_date_ts = pd.to_datetime(selected_date)
        df['date'] = selected_date_ts
        df['Week'] = df['date'].dt.strftime('%Y-%W')
        df['Month'] = df['date'].dt.strftime('%Y-%m')
        
        # Handle duplicates by grouping and summing correctly
        df['record_id'] = df.apply(
            lambda x: f"{x['Brand']}{x['Category']}{x['Size']}{x['Color']}{x['Month']}", 
            axis=1
        )
        log_output.info("Checking for duplicates in uploaded file...")
        before_dedup = len(df)
        
        if df['record_id'].duplicated().any():
            log_output.info(f"Found {df['record_id'].duplicated().sum()} duplicate record_ids to process")
            final_df = df.groupby('record_id').agg({
                'Brand': 'first', 'Category': 'first', 'Size': 'first', 'MRP': 'first',
                'Color': 'first', 'SalesQty': 'sum', 'PurchaseQty': 'sum', 'date': 'first',
                'Week': 'first', 'Month': 'first'
            }).reset_index(drop=True)
            log_output.info(f"After grouping duplicates - SalesQty sum: {final_df['SalesQty'].sum()}, PurchaseQty sum: {final_df['PurchaseQty'].sum()}")
        else:
            final_df = df.drop('record_id', axis=1)
            log_output.info("No duplicates found")
        
        after_dedup = len(final_df)
        log_output.info(f"Reduced to {after_dedup} unique records from {before_dedup} total rows")
        
        # Calculate grand total after preprocessing
        total_sales = int(final_df['SalesQty'].sum())
        total_purchases = int(final_df['PurchaseQty'].sum())
        log_output.info(f"Calculated grand totals - SalesQty: {total_sales}, PurchaseQty: {total_purchases}")
        
        grand_total_row = pd.DataFrame({
            'Brand': ['grand total'], 'Category': [''], 'Size': [''], 'MRP': [0.0], 
            'Color': [''], 'SalesQty': [total_sales], 'PurchaseQty': [total_purchases], 
            'date': [selected_date_ts], 'Week': [selected_date_ts.strftime('%Y-%W')], 
            'Month': [selected_date_ts.strftime('%Y-%m')]
        })
        
        # Ensure Grand Total is at the top
        final_df_with_total = pd.concat([grand_total_row, final_df], ignore_index=True)
        
        log_output.info(f"Preprocessing complete with grand total at top. Final shape: {final_df_with_total.shape}")
        return final_df_with_total
    
    except Exception as e:
        log_output.error(f"Error during preprocessing: {str(e)}")
        return None

# Function to save preprocessed file with YYMMDD format
def save_preprocessed_file(df, selected_date, log_output):
    """Save the preprocessed dataframe as Excel file with YYMMDD format"""
    try:
        date_str = selected_date.strftime('%y%m%d')
        file_name = f"salesninventory_{date_str}.xlsx"
        file_path = os.path.join(PROCESSED_DIR, file_name)
        
        df.to_excel(file_path, index=False)
        log_output.info(f"Preprocessed file saved: {file_name}")
        
        df_no_total = df[df['Brand'] != 'grand total'].copy()
        update_master_summary(df_no_total, log_output)
        
        return file_path
    except Exception as e:
        log_output.error(f"Error saving preprocessed file: {str(e)}")
        return None

# Updated function to update master summary
def update_master_summary(new_df, log_output):
    """Update master_summary.xlsx with new data, tracking 30 days, archiving monthly"""
    master_file = os.path.join(PROCESSED_DIR, MASTER_SUMMARY_FILE)
    cutoff_date = pd.Timestamp(datetime.now() - timedelta(days=30))
    current_month = pd.Timestamp.now().strftime('%Y-%m')
    
    try:
        if os.path.exists(master_file):
            master_df = pd.read_excel(master_file)
            master_df['date'] = pd.to_datetime(master_df['date'])
            non_grand_total = master_df[master_df['Brand'] != 'grand total']
            if not non_grand_total.empty:
                first_date = non_grand_total['date'].iloc[0]
                file_month = first_date.strftime('%Y-%m')
                if file_month != current_month:
                    archive_file = os.path.join(PROCESSED_DIR, f"master_summary_{file_month}.xlsx")
                    shutil.move(master_file, archive_file)
                    log_output.info(f"Archived {MASTER_SUMMARY_FILE} to {archive_file}")
                    master_df = pd.DataFrame(columns=new_df.columns)
            else:
                master_df = pd.DataFrame(columns=new_df.columns)
        else:
            master_df = pd.DataFrame(columns=new_df.columns)
        
        master_df = master_df[master_df['Brand'] != 'grand total']
        master_df = master_df[master_df['date'] >= cutoff_date]
        
        combined_df = pd.concat([master_df, new_df], ignore_index=True)
        
        combined_df['record_id'] = combined_df.apply(
            lambda x: f"{x['Brand']}{x['Category']}{x['Size']}{x['Color']}{x['Month']}", 
            axis=1
        )
        
        final_df = combined_df.groupby('record_id').agg({
            'Brand': 'first', 'Category': 'first', 'Size': 'first', 'MRP': 'first',
            'Color': 'first', 'SalesQty': 'sum', 'PurchaseQty': 'sum', 'date': 'max',
            'Week': 'first', 'Month': 'first'
        }).reset_index(drop=True)
        
        final_df = final_df.sort_values('date', ascending=False)
        
        total_sales = int(final_df['SalesQty'].sum())
        total_purchases = int(final_df['PurchaseQty'].sum())
        grand_total_row = pd.DataFrame({
            'Brand': ['grand total'], 'Category': [''], 'Size': [''], 'MRP': [0.0], 
            'Color': [''], 'SalesQty': [total_sales], 'PurchaseQty': [total_purchases], 
            'date': [pd.Timestamp.now()], 'Week': [pd.Timestamp.now().strftime('%Y-%W')], 
            'Month': [pd.Timestamp.now().strftime('%Y-%m')]
        })
        master_df = pd.concat([grand_total_row, final_df], ignore_index=True)
        
        master_df.to_excel(master_file, index=False)
        log_output.info(f"Master summary updated. Rows: {len(master_df)}, Sales: {total_sales}, Purchases: {total_purchases}")
        return True
    
    except Exception as e:
        log_output.error(f"Error updating master summary: {str(e)}")
        return False

# Function to enforce retention policy (keep 7 files)
def enforce_retention_policy(log_output):
    """Keep exactly 7 most recent XLSX files, delete oldest if more than 7"""
    try:
        files = glob.glob(os.path.join(PROCESSED_DIR, "salesninventory_*.xlsx"))
        files.sort()
        
        if len(files) > 7:
            files_to_delete = files[:-7]
            for file in files_to_delete:
                filename = os.path.basename(file)
                if not filename.startswith("salesninventory_") or not filename.endswith(".xlsx"):
                    log_output.warning(f"Skipping invalid filename: {filename}")
                    continue
                prefix_len = len("salesninventory_")
                date_str = filename[prefix_len:prefix_len+6]
                if len(date_str) != 6 or not date_str.isdigit():
                    log_output.warning(f"Skipping file with invalid date format: {filename} (date_str: '{date_str}')")
                    continue
                os.remove(file)
                log_output.info(f"Deleted old file (exceeded 7-file limit): {filename}")
        
        return True
    except Exception as e:
        log_output.error(f"Error enforcing retention policy: {str(e)}")
        return False

# Updated function to upload data to Neon DB with month-specific logic
def upload_to_database(df, selected_date, log_output):
    """Upload preprocessed data to Neon DB, treating first upload of new month as new records"""
    conn = get_db_connection()
    if not conn:
        log_output.error("Failed to connect to database")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Ensure table exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_data (
            id SERIAL PRIMARY KEY,
            brand VARCHAR(100),
            category VARCHAR(100),
            size VARCHAR(50),
            mrp FLOAT,
            color VARCHAR(50),
            week VARCHAR(10),
            month VARCHAR(10),
            sales_qty INTEGER,
            purchase_qty INTEGER,
            created_at TIMESTAMP
        )
        """)
        cursor.execute("DROP INDEX IF EXISTS idx_sales_lookup")
        cursor.execute("""
        CREATE INDEX idx_sales_lookup 
        ON sales_data (brand, category, size, color, month)
        """)
        conn.commit()
        
        # Get the month of the uploaded data
        upload_month = pd.to_datetime(selected_date).strftime('%Y-%m')
        
        # Check if there’s any data for this month already
        cursor.execute("""
        SELECT COUNT(*) 
        FROM sales_data 
        WHERE month = %s AND brand != 'grand total'
        """, (upload_month,))
        month_count = cursor.fetchone()[0]
        
        # Total record count to determine if it’s the first ever upload
        cursor.execute("SELECT COUNT(*) FROM sales_data WHERE brand != 'grand total'")
        total_count = cursor.fetchone()[0]
        
        if total_count == 0:
            log_output.info("First ever upload - inserting all records")
            upload_using_copy(df, selected_date, log_output)
            new_records = len(df[df['Brand'] != 'grand total'])
            updated_records = 0
        elif month_count == 0:
            log_output.info(f"First upload for new month {upload_month} - inserting all records")
            upload_using_copy(df, selected_date, log_output)
            new_records = len(df[df['Brand'] != 'grand total'])
            updated_records = 0
        else:
            log_output.info(f"Updating records for existing month {upload_month}")
            new_records, updated_records = merge_data_with_existing(df, selected_date, log_output)
        
        conn.close()
        update_neon_grand_total(log_output)
        log_output.info(f"Upload complete. Added {new_records} new, updated {updated_records}")
        return {"new": new_records, "updated": updated_records}
    
    except Exception as e:
        log_output.error(f"Database upload error: {str(e)}")
        if conn:
            conn.close()
        return False

# Use COPY command for initial upload with user-defined date
def upload_using_copy(df, selected_date, log_output):
    """Use SQLAlchemy for efficient bulk data upload with user-defined date"""
    try:
        engine = get_sqlalchemy_engine()
        db_df = df[df['Brand'] != 'grand total'][['Brand', 'Category', 'Size', 'MRP', 'Color', 'Week', 'Month', 
                                                  'SalesQty', 'PurchaseQty', 'date']].copy()
        db_df.columns = ['brand', 'category', 'size', 'mrp', 'color', 'week', 'month', 
                         'sales_qty', 'purchase_qty', 'created_at']
        db_df['created_at'] = pd.to_datetime(selected_date)
        
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sales_data_temp"))
            conn.execute(text("""
                CREATE TEMP TABLE sales_data_temp (
                    brand VARCHAR(100),
                    category VARCHAR(100),
                    size VARCHAR(50),
                    mrp FLOAT,
                    color VARCHAR(50),
                    week VARCHAR(10),
                    month VARCHAR(10),
                    sales_qty INTEGER,
                    purchase_qty INTEGER,
                    created_at TIMESTAMP
                )
            """))
        
        db_df.to_sql('sales_data_temp', engine, if_exists='append', index=False)
        
        with engine.begin() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM sales_data_temp"))
            total_count = result.scalar()
            conn.execute(text("""
                INSERT INTO sales_data 
                (brand, category, size, mrp, color, week, month, sales_qty, purchase_qty, created_at)
                SELECT * FROM sales_data_temp
            """))
            conn.execute(text("DROP TABLE IF EXISTS sales_data_temp"))
        
        log_output.info(f"Inserted {total_count} records into sales_data")
        return {"new": total_count, "updated": 0}
    
    except Exception as e:
        log_output.error(f"Error during bulk upload: {str(e)}")
        return False

# Merge data with existing records, summing quantities for duplicates within the same month
def merge_data_with_existing(df, selected_date, log_output):
    """Merge new data with existing data in the database for the same month, summing quantities"""
    conn = get_db_connection()
    if not conn:
        log_output.error("Failed to connect to database")
        return 0, 0
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
        DROP TABLE IF EXISTS temp_sales_data;
        CREATE TABLE temp_sales_data (
            brand VARCHAR(100),
            category VARCHAR(100),
            size VARCHAR(50),
            mrp FLOAT,
            color VARCHAR(50),
            week VARCHAR(10),
            month VARCHAR(10),
            sales_qty INTEGER,
            purchase_qty INTEGER,
            created_at TIMESTAMP
        )
        """)
        
        df_no_total = df[df['Brand'] != 'grand total'].copy()
        selected_date_ts = pd.to_datetime(selected_date)
        upload_month = selected_date_ts.strftime('%Y-%m')
        temp_data = [
            (row['Brand'], row['Category'], row['Size'], float(row['MRP']), 
             row['Color'], row['Week'], row['Month'], int(row['SalesQty']), 
             int(row['PurchaseQty']), selected_date_ts) for _, row in df_no_total.iterrows()
        ]
        psycopg2.extras.execute_batch(
            cursor,
            """
            INSERT INTO temp_sales_data 
            (brand, category, size, mrp, color, week, month, sales_qty, purchase_qty, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            temp_data,
            page_size=1000
        )
        
        # Update only records for the current month
        cursor.execute("""
        UPDATE sales_data s
        SET 
            sales_qty = s.sales_qty + t.sales_qty,
            purchase_qty = s.purchase_qty + t.purchase_qty,
            created_at = t.created_at,
            week = t.week,
            month = t.month,
            mrp = t.mrp
        FROM temp_sales_data t
        WHERE s.brand = t.brand AND s.category = t.category AND 
              s.size = t.size AND s.color = t.color AND s.month = t.month
              AND s.month = %s AND s.brand != 'grand total'
        """, (upload_month,))
        updated_records = cursor.rowcount
        log_output.info(f"Updated {updated_records} existing records with summed quantities for month {upload_month}")
        
        # Insert new records for the current month
        cursor.execute("""
        INSERT INTO sales_data 
        (brand, category, size, mrp, color, week, month, sales_qty, purchase_qty, created_at)
        SELECT t.brand, t.category, t.size, t.mrp, t.color, t.week, t.month, 
               t.sales_qty, t.purchase_qty, t.created_at
        FROM temp_sales_data t
        LEFT JOIN sales_data s
        ON s.brand = t.brand AND s.category = t.category AND 
           s.size = t.size AND s.color = t.color AND s.month = t.month
        WHERE s.id IS NULL
        """)
        new_records = cursor.rowcount
        log_output.info(f"Inserted {new_records} new records for month {upload_month}")
        
        cursor.execute("DROP TABLE temp_sales_data")
        conn.commit()
        conn.close()
        
        return new_records, updated_records
    
    except Exception as e:
        log_output.error(f"Error during merge: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()
        return 0, 0

# Clean up Neon DB records older than 3 years
def cleanup_old_db_records(log_output):
    """Remove Neon DB records older than 3 years"""
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM sales_data WHERE created_at < NOW() - INTERVAL '3 years' AND brand != 'grand total'")
            deleted = cursor.rowcount
            conn.commit()
            conn.close()
            if deleted > 0:
                log_output.info(f"Deleted {deleted} records older than 3 years from Neon DB")
        except Exception as e:
            log_output.error(f"Error cleaning up Neon DB: {str(e)}")
            conn.close()

# Update Neon DB with grand total using latest timestamp
def update_neon_grand_total(log_output):
    """Update Neon DB with a grand total row using current timestamp"""
    conn = get_db_connection()
    if not conn:
        log_output.error("Failed to connect to database for grand total update")
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM sales_data WHERE brand = 'grand total'")
        conn.commit()
        
        cursor.execute("""
        SELECT COALESCE(SUM(sales_qty), 0) as total_sales, 
               COALESCE(SUM(purchase_qty), 0) as total_purchases 
        FROM sales_data WHERE brand != 'grand total'
        """)
        result = cursor.fetchone()
        total_sales = int(result[0])
        total_purchases = int(result[1])
        
        current_time = pd.Timestamp.now()
        cursor.execute("""
        INSERT INTO sales_data (brand, category, size, mrp, color, week, month, sales_qty, purchase_qty, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, ('grand total', '', '', 0.0, '', current_time.strftime('%Y-%W'), 
              current_time.strftime('%Y-%m'), total_sales, total_purchases, 
              current_time))
        
        conn.commit()
        conn.close()
        log_output.info(f"Updated Neon DB with grand total: Sales={total_sales}, Purchases={total_purchases}, Timestamp={current_time}")
    except Exception as e:
        log_output.error(f"Error updating Neon DB grand total: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()

# Get data from Neon DB for preview with Grand Total pinned to top
def get_database_preview(log_output):
    """Get latest 1000 records from Neon DB, ensuring Grand Total is first"""
    try:
        engine = get_sqlalchemy_engine()
        query = """
        SELECT * FROM sales_data 
        ORDER BY CASE WHEN brand = 'grand total' THEN 0 ELSE 1 END, created_at DESC 
        LIMIT 1000
        """
        df = pd.read_sql(query, engine)
        log_output.info(f"Retrieved {len(df)} latest records from Neon DB")
        return df
    except Exception as e:
        log_output.error(f"Error getting preview: {str(e)}")
        return pd.DataFrame()

# Get aggregated data for visualizations with date filters
def get_visualization_data(log_output, start_date=None, end_date=None):
    """Get aggregated data from Neon DB with optional date range"""
    try:
        engine = get_sqlalchemy_engine()
        where_clause = "WHERE created_at BETWEEN %s AND %s AND brand != 'grand total'" if start_date and end_date else "WHERE brand != 'grand total'"
        params = (start_date, end_date) if start_date and end_date else ()
        
        brand_query = f"""
        SELECT brand, SUM(sales_qty) as total_sales, SUM(purchase_qty) as total_purchases
        FROM sales_data {where_clause}
        GROUP BY brand ORDER BY total_sales DESC LIMIT 10
        """
        brand_df = pd.read_sql(brand_query, engine, params=params)
        
        category_query = f"""
        SELECT category, SUM(sales_qty) as total_sales, SUM(purchase_qty) as total_purchases
        FROM sales_data {where_clause}
        GROUP BY category ORDER BY total_sales DESC LIMIT 10
        """
        category_df = pd.read_sql(category_query, engine, params=params)
        
        monthly_query = f"""
        SELECT month, SUM(sales_qty) as total_sales, SUM(purchase_qty) as total_purchases
        FROM sales_data {where_clause}
        GROUP BY month ORDER BY month
        """
        monthly_df = pd.read_sql(monthly_query, engine, params=params)
        
        weekly_query = f"""
        SELECT week, SUM(sales_qty) as total_sales, SUM(purchase_qty) as total_purchases
        FROM sales_data {where_clause}
        GROUP BY week ORDER BY week
        """
        weekly_df = pd.read_sql(weekly_query, engine, params=params)
        
        log_output.info("Retrieved aggregated data for visualizations")
        return {"brand": brand_df, "category": category_df, "monthly": monthly_df, "weekly": weekly_df}
    except Exception as e:
        log_output.error(f"Error getting viz data: {str(e)}")
        return {}

# Create visualizations
def create_visualizations(data):
    """Create visualizations using Plotly with enhanced styling"""
    visualizations = {}
    
    if not data:
        return visualizations
    
    if "brand" in data and not data["brand"].empty:
        brand_fig = px.bar(
            data["brand"], x="brand", y=["total_sales", "total_purchases"],
            title="Top 10 Brands by Sales and Purchases", barmode="group",
            color_discrete_map={"total_sales": "#00CC96", "total_purchases": "#EF553B"},
            labels={"value": "Quantity", "variable": "Metric"}
        )
        brand_fig.update_traces(hovertemplate="%{x}<br>%{y} %{variable}")
        visualizations["brand"] = brand_fig
    
    if "category" in data and not data["category"].empty:
        category_fig = px.bar(
            data["category"], x="category", y=["total_sales", "total_purchases"],
            title="Top 10 Categories by Sales and Purchases", barmode="group",
            color_discrete_map={"total_sales": "#00CC96", "total_purchases": "#EF553B"},
            labels={"value": "Quantity", "variable": "Metric"}
        )
        category_fig.update_traces(hovertemplate="%{x}<br>%{y} %{variable}")
        visualizations["category"] = category_fig
    
    if "monthly" in data and not data["monthly"].empty:
        monthly_fig = px.line(
            data["monthly"], x="month", y=["total_sales", "total_purchases"],
            title="Monthly Sales and Purchase Trends",
            color_discrete_map={"total_sales": "#00CC96", "total_purchases": "#EF553B"}
        )
        visualizations["monthly"] = monthly_fig
    
    if "weekly" in data and not data["weekly"].empty:
        weekly_fig = px.line(
            data["weekly"], x="week", y=["total_sales", "total_purchases"],
            title="Weekly Sales and Purchase Trends",
            color_discrete_map={"total_sales": "#00CC96", "total_purchases": "#EF553B"}
        )
        visualizations["weekly"] = weekly_fig
    
    return visualizations

# Custom logger for Streamlit
class StreamlitLogger:
    def __init__(self, placeholder):
        self.placeholder = placeholder
        self.logs = []
    
    def info(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[INFO] {timestamp} - {message}"
        self.logs.append(log_entry)
        self.update_display()
        logger.info(message)
    
    def error(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[ERROR] {timestamp} - {message}"
        self.logs.append(log_entry)
        self.update_display()
        logger.error(message)
    
    def warning(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[WARNING] {timestamp} - {message}"
        self.logs.append(log_entry)
        self.update_display()
        logger.warning(message)
    
    def update_display(self):
        log_text = "\n".join(self.logs[-20:])
        self.placeholder.code(log_text, language="bash")

# Chatbot functions

# ... (keep all other imports and code before load_processed_data unchanged) ...

# Chatbot functions

def normalize_columns(df):
    """Normalize column names to lowercase"""
    df.columns = [col.lower() for col in df.columns]
    return df

@lru_cache(maxsize=1)
def load_processed_data():
    """Load processed data from master_summary.xlsx and daily sales files"""
    try:
        start_time = time.time()
        master_summary = pd.read_excel(os.path.join(PROCESSED_DIR, "master_summary.xlsx"), dtype={
            'salesqty': int, 'purchaseqty': int, 'mrp': float
        })
        master_summary = normalize_columns(master_summary)
        master_summary['date'] = pd.to_datetime(master_summary['date'], errors='coerce')
       
        # Use glob.glob() to find files, not just glob
        daily_files = glob.glob(os.path.join(PROCESSED_DIR, "salesninventory_*.xlsx"))
        daily_data = [pd.read_excel(file, dtype={'salesqty': int, 'purchaseqty': int, 'mrp': float}) for file in daily_files]
        daily_data_combined = pd.concat(daily_data, ignore_index=True)
        daily_data_combined = normalize_columns(daily_data_combined)
        daily_data_combined['date'] = pd.to_datetime(daily_data_combined['date'], errors='coerce')
       
        master_summary = master_summary.sort_values('date', ascending=False)
        daily_data_combined = daily_data_combined.sort_values('date', ascending=False)
       
        logger.info(f"Loaded processed data in {time.time() - start_time:.2f} seconds")
        return master_summary, daily_data_combined
    except Exception as e:
        logger.error(f"Error loading processed data: {e}")
        raise

@lru_cache(maxsize=1)
def load_neon_data():
    """Load data from Neon DB for the last 3 years"""
    try:
        start_time = time.time()
        current_date = datetime.now()
        three_years_ago = current_date.replace(year=current_date.year - 3)
       
        query = """
            SELECT * FROM sales_data
            WHERE created_at >= %s
            ORDER BY created_at DESC
            LIMIT 10000
        """
        params = (three_years_ago.strftime('%Y-%m-%d'),)
        engine = get_sqlalchemy_engine()
        neon_data = pd.read_sql(query, engine, params=params, dtype={
            'sales_qty': int, 'purchase_qty': int, 'mrp': float
        })
        neon_data = normalize_columns(neon_data)
        neon_data['created_at'] = pd.to_datetime(neon_data['created_at'], errors='coerce')
       
        if neon_data.empty:
            logger.warning("Neon DB returned empty data")
            return pd.DataFrame(columns=['brand', 'category', 'size', 'sales_qty', 'purchase_qty', 'created_at'])
       
        neon_data = neon_data.sort_values('created_at', ascending=False)
        logger.info(f"Loaded Neon DB data in {time.time() - start_time:.2f} seconds")
        return neon_data
    except Exception as e:
        logger.error(f"Error loading Neon DB data: {e}")
        return pd.DataFrame(columns=['brand', 'category', 'size', 'sales_qty', 'purchase_qty', 'created_at'])

# ... (keep all other functions and the UI unchanged) ...

def retrieve_relevant_data(query, master_summary, daily_data, neon_data):
    """Retrieve relevant data based on the user's query with trends and seasonality"""
    try:
        start_time = time.time()
        current_date = datetime.now()
        current_month = current_date.strftime("%Y-%m")
        current_week = int(current_date.strftime("%W"))  # Extract week number as integer

        if "week" in query.lower():
            relevant_data = daily_data[daily_data['date'].dt.isocalendar().week == current_week]
            source = "Current Week Data (Sales Inventory Files)"
        elif "month" in query.lower():
            relevant_data = master_summary[master_summary["month"] == current_month]
            source = "Monthly Inventory Data (Master Summary)"
        elif "quarter" in query.lower():
            quarter_start = current_date.replace(month=((current_date.month - 1) // 3 * 3 + 1), day=1)
            relevant_data = neon_data[neon_data["created_at"].dt.to_period('Q') == quarter_start.to_period('Q')]
            source = "Quarterly Historical Data (Neon DB)"
        else:
            relevant_data = neon_data
            source = "Historical and Current Data (Neon DB)"

        # Ensure relevant_data is not empty and has required columns
        if relevant_data.empty or not all(col in relevant_data.columns for col in ['sales_qty', 'purchase_qty', 'brand', 'category', 'size']):
            logger.warning("Relevant data is empty or missing required columns")
            return pd.DataFrame(columns=['brand', 'category', 'size', 'sales_qty', 'purchase_qty']), source, {}, pd.DataFrame()

        # Perform full dataset analysis for trends and seasonality
        trend_data = relevant_data.copy()
        trend_data = trend_data.sort_values(['brand', 'category', 'size', 'date' if 'date' in trend_data.columns else 'created_at'], ascending=True)

        # Trend Analysis: 7-day (weekly) or 30-day (monthly/quarterly) moving average
        if "week" in query.lower():
            trend_window = 7
        else:
            trend_window = 30
        trend_data['ma_sales'] = trend_data['sales_qty'].rolling(window=trend_window, min_periods=1).mean()
        trend_data['growth_rate'] = trend_data['sales_qty'].pct_change() * 100  # Week-over-week or month-over-month growth

        # Seasonal Analysis: Group by month or quarter for patterns
        if "quarter" in query.lower():
            seasonal_data = trend_data.groupby([trend_data['created_at'].dt.to_period('Q'), 'brand', 'category', 'size'])['sales_qty'].sum().reset_index()
            seasonal_data['seasonal_avg'] = seasonal_data.groupby(['brand', 'category', 'size'])['sales_qty'].transform('mean')
        elif "month" in query.lower():
            seasonal_data = trend_data.groupby([trend_data['date'].dt.to_period('M'), 'brand', 'category', 'size'])['sales_qty'].sum().reset_index()
            seasonal_data['seasonal_avg'] = seasonal_data.groupby(['brand', 'category', 'size'])['sales_qty'].transform('mean')
        else:
            seasonal_data = pd.DataFrame()  # No seasonal analysis for weekly

        # Vectorized filtering for efficiency, using normalized column names
        if "75%" in query.lower() or "50%" in query.lower():
            relevant_data["soldpercentage"] = (relevant_data["sales_qty"] / (relevant_data["sales_qty"] + relevant_data["purchase_qty"].fillna(0))) * 100
            relevant_data = relevant_data[relevant_data["soldpercentage"].between(50, 75) | (relevant_data["soldpercentage"] >= 75)]
        elif "best-selling" in query.lower():
            relevant_data = relevant_data.groupby(["brand", "category", "size"], as_index=False)["sales_qty"].sum().sort_values("sales_qty", ascending=False)
        elif "non-moving" in query.lower() or "slow-moving" in query.lower():
            relevant_data = relevant_data[relevant_data["sales_qty"] == 0].copy()  # Use .copy()
            relevant_data["agingdays"] = (current_date - relevant_data["created_at"]).dt.days
        elif "top 20%" in query.lower():
            total_sales = relevant_data["sales_qty"].sum()
            relevant_data = (relevant_data.groupby(["brand", "category", "size"], as_index=False)["sales_qty"].sum()
                           .sort_values("sales_qty", ascending=False))
            relevant_data["cumulativesales"] = relevant_data["sales_qty"].cumsum() / total_sales * 100
            relevant_data = relevant_data[relevant_data["cumulativesales"] <= 80]
        elif "exchanges" in query.lower() or "returns" in query.lower():
            relevant_data = relevant_data.sort_values("sales_qty", ascending=True).copy()  # Use .copy()
            relevant_data["potentialreturns"] = (relevant_data["sales_qty"] * 0.075).round()  # Assume 7.5% return rate
            relevant_data["turnarounddays"] = 10  # Standard 7–14 day turnaround
        elif "rejected goods" in query.lower():
            relevant_data = relevant_data[relevant_data["sales_qty"] == 0].copy()  # Use .copy()
            relevant_data["potentialrejections"] = (relevant_data["purchase_qty"] * 0.035).round()  # Assume 3.5% rejection rate
            relevant_data["rejectiondays"] = 10  # Standard 7–14 day rejection timeline
        elif "variances" in query.lower():
            relevant_data["sales_variance"] = relevant_data["sales_qty"].rolling(window=trend_window, min_periods=1).std()
            relevant_data["stock_variance"] = relevant_data["purchase_qty"].rolling(window=trend_window, min_periods=1).std()

        # Prepare summary data for output (top 3 for table, but use full data for analysis)
        summary_data = relevant_data.dropna(subset=['sales_qty', 'purchase_qty'], how='all').head(3)  # Limit table to 3 for brevity
        trend_summary = trend_data[['brand', 'category', 'size', 'ma_sales', 'growth_rate']].dropna().head(3)
        seasonal_summary = seasonal_data[['brand', 'category', 'size', 'sales_qty', 'seasonal_avg']].dropna().head(3) if not seasonal_data.empty else pd.DataFrame()

        # Store trends and seasonality for the response
        trends = {
            'moving_avg': trend_summary['ma_sales'].mean() if not trend_summary.empty else 0,
            'growth_rate': trend_summary['growth_rate'].mean() if not trend_summary.empty else 0,
            'seasonal_pattern': seasonal_summary['seasonal_avg'].mean() if not seasonal_summary.empty else 0
        }

        logger.info(f"Retrieved relevant data in {time.time() - start_time:.2f} seconds")
        return summary_data, source, trends, seasonal_summary
    except Exception as e:
        logger.error(f"Error retrieving relevant data: {e}")
        return pd.DataFrame(columns=['brand', 'category', 'size', 'sales_qty', 'purchase_qty']), "Fallback Data Source (No Data Available)", {}, pd.DataFrame()

@lru_cache(maxsize=10)
def generate_response_cached(query, relevant_data_str, source, trends, seasonal_summary):
    """Generate a response using the Ollama model based on the query and data"""
    try:
        start_time = time.time()
        current_date = datetime.now().strftime("%B %d, %Y")
        trend_info = f"Moving average sales: {trends['moving_avg']:.2f} units, Growth rate: {trends['growth_rate']:.2f}%, " + \
                     f"Seasonal pattern: {trends['seasonal_pattern']:.2f} units" if trends['moving_avg'] or trends['growth_rate'] or trends['seasonal_pattern'] else ""
        seasonal_info = seasonal_summary.to_string(index=False) if not seasonal_summary.empty else ""

        prompt = f"""
        User Question: {query}
        Relevant Data (from {source}):
        {relevant_data_str}
        Trend Analysis: {trend_info}
        Seasonal Analysis: {seasonal_info}
       
        Based on the provided data only, provide a brief, easy-to-read business snapshot as a summary in plain, normal language. Present it as if written by a professional data scientist, business sales and inventory analyst, and marketing expert. Use the following structure:
        - Title: "Inventory and Sales Quick Look: [User Question Topic]"
        - Subtitle: Date (e.g., "March 5, 2025")
        - "What We Noticed": Summarize the key findings in simple terms, including a table with up to 3 relevant items (Brand, Category, Size, Sales Qty, Purchase Qty, Status, PotentialReturns, TurnaroundDays, PotentialRejections, or RejectionDays, depending on the question). Use only the data provided in relevant_data_str, avoiding fabricated brands or data not present in the input.
        - "What This Means for Us": Explain the implications in plain language, including sales trends, sell-out estimates, or inferred return/exchange/rejection timelines, using confident, data-driven assumptions based on the provided data, trends, and seasonal patterns.
        - "What We Should Do": Provide 3 numbered, practical recommendations for sales, inventory, and marketing, tailored to a general audience, leveraging trends, seasonality, and only the provided data.
        - "Where We Got This": State the data source in simple terms.

        Ensure the response is concise, visually appealing, and understandable for a normal person, avoiding technical jargon. Use tables, numbered lists, and bullet points (•) for clarity, and avoid using # or *.
        """
       
        response = ollama.generate(model="llama3.2:1b", prompt=prompt, options={"temperature": 0.7, "top_p": 0.9})
        logger.info(f"Generated response in {time.time() - start_time:.2f} seconds")
        return response["response"]
    except Exception as e:
        logger.error(f"Error generating response: {e}")
        return f"Inventory and Sales Quick Look: {query.split('?')[0]}\n{current_date}\nAn error occurred while processing your request. Please check the data or try again later."

def run_chatbot(query, log_output):
    """Run the chatbot with the user's query and log progress"""
    try:
        log_output.info("Loading processed data...")
        start_time = time.time()
        master_summary, daily_data = load_processed_data()
        log_output.info(f"Loaded processed data in {time.time() - start_time:.2f} seconds")

        log_output.info("Loading Neon DB data...")
        start_time = time.time()
        neon_data = load_neon_data()
        log_output.info(f"Loaded Neon DB data in {time.time() - start_time:.2f} seconds")

        log_output.info("Retrieving relevant data...")
        start_time = time.time()
        relevant_data, source, trends, seasonal_summary = retrieve_relevant_data(query, master_summary, daily_data, neon_data)
        log_output.info(f"Retrieved relevant data in {time.time() - start_time:.2f} seconds")

        log_output.info("Generating response...")
        start_time = time.time()
        relevant_data_str = relevant_data.to_string(index=False) if not relevant_data.empty else "No relevant data found."
        response = generate_response_cached(query, relevant_data_str, source, trends, seasonal_summary)
        log_output.info(f"Generated response in {time.time() - start_time:.2f} seconds")

        return response
    except Exception as e:
        log_output.error(f"Error in chatbot: {e}")
        return f"Inventory and Sales Quick Look: {query.split('?')[0]}\n{datetime.now().strftime('%B %d, %Y')}\nAn error occurred while processing your request. Please check the data or try again later."

# Streamlit UI
def ui():
    """Main Streamlit UI function"""
    st.title("Sales & Inventory Data Pipeline")
    
    st.sidebar.header("Get Started")
    st.sidebar.write("Upload your sales data and pick a date to process it!")
    uploaded_file = st.sidebar.file_uploader("Choose an Excel File", type=["xlsx"], 
                                            help="Upload your sales data file here")
    selected_date = st.sidebar.date_input("Pick a Date for the Data", datetime.now(), 
                                          help="This date will be assigned to your data")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Pipeline", "Database Preview", "Visualizations", "Chatbot"])
    
    with tab1:
        st.subheader("Process Your Data")
        st.write("Upload and process your sales and inventory data here.")
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.write("**What’s Happening?**")
            log_placeholder = st.empty()
            log_output = StreamlitLogger(log_placeholder)
            st.write("Watch the progress below:")
            progress_bar = st.progress(0)
            
            if 'results' not in st.session_state:
                st.session_state.results = None
            
            process_button = st.button("Process My Data", help="Click to start processing your file")
            
            if process_button and uploaded_file is not None:
                st.session_state.results = None
                progress_bar.progress(10)
                log_output.info("Starting your file…")
                
                temp_file_path = os.path.join(TEMP_STORAGE_DIR, uploaded_file.name)
                with open(temp_file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                log_output.info(f"File saved temporarily: {uploaded_file.name}")
                progress_bar.progress(20)
                
                df = preprocess_data(temp_file_path, selected_date, log_output)
                if df is not None:
                    progress_bar.progress(40)
                    preprocessed_path = save_preprocessed_file(df, selected_date, log_output)
                    if preprocessed_path:
                        progress_bar.progress(60)
                        enforce_retention_policy(log_output)
                        progress_bar.progress(70)
                        results = upload_to_database(df, selected_date, log_output)
                        if results:
                            progress_bar.progress(90)
                            st.session_state.results = {
                                "new_records": results["new"],
                                "updated_records": results["updated"],
                                "total_records": len(df),
                                "date": selected_date.strftime("%Y-%m-%d")
                            }
                            log_output.info("Data pipeline process completed successfully!")
                            progress_bar.progress(100)
                        else:
                            log_output.error("Failed to upload data to database")
                            progress_bar.progress(0)
                    else:
                        log_output.error("Failed to save preprocessed file")
                        progress_bar.progress(0)
                else:
                    log_output.error("Failed to preprocess data")
                    progress_bar.progress(0)
                
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
        
        with col2:
            st.write("**Results**")
            if st.session_state.results:
                results = st.session_state.results
                master_file = os.path.join(PROCESSED_DIR, MASTER_SUMMARY_FILE)
                if os.path.exists(master_file):
                    master_df = pd.read_excel(master_file)
                    grand_total_row = master_df[master_df['Brand'] == 'grand total'].iloc[0]
                    local_total_sales = int(grand_total_row['SalesQty'])
                    local_total_purchases = int(grand_total_row['PurchaseQty'])
                else:
                    local_total_sales, local_total_purchases = 0, 0
                
                date_str = selected_date.strftime('%y%m%d')
                file_name = f"salesninventory_{date_str}.xlsx"
                file_path = os.path.join(PROCESSED_DIR, file_name)
                if os.path.exists(file_path):
                    daily_df = pd.read_excel(file_path)
                    daily_grand_row = daily_df[daily_df['Brand'] == 'grand total'].iloc[0]
                    daily_total_sales = int(daily_grand_row['SalesQty'])
                    daily_total_purchases = int(daily_grand_row['PurchaseQty'])
                else:
                    daily_total_sales, daily_total_purchases = 0, 0
                
                st.success("🎉 Processing Completed!")
                st.metric("Daily Grand Total", 
                          f"Sales: {daily_total_sales:,} | Purchases: {daily_total_purchases:,}")
                st.metric("Master Grand Total (Last 30 Days)", 
                          f"Sales: {local_total_sales:,} | Purchases: {local_total_purchases:,}")
                st.write(f"*Date Processed:* {results['date']}")
                st.write(f"*Total Records Processed:* {results['total_records']}")
                st.write(f"*New Records Added:* {results['new_records']}")
                st.write(f"*Records Updated:* {results['updated_records']}")
                
                if os.path.exists(file_path):
                    with open(file_path, "rb") as f:
                        st.download_button(
                            label="Download Processed File",
                            data=f, file_name=file_name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            help="Download today’s processed file"
                        )
            else:
                st.info("Upload a file and click 'Process My Data' to begin!")
    
    with tab2:
        st.subheader("Database Preview")
        st.write("View the latest data stored in the cloud database.")
        refresh_button = st.button("Refresh Data", help="Update with the latest data")
        
        log_placeholder = st.empty()
        log_output = StreamlitLogger(log_placeholder)
        
        if refresh_button or st.session_state.get('results'):
            preview_df = get_database_preview(log_output)
            if not preview_df.empty:
                neon_grand_row = preview_df[preview_df['brand'] == 'grand total'].iloc[0]
                neon_total_sales = int(neon_grand_row['sales_qty'])
                neon_total_purchases = int(neon_grand_row['purchase_qty'])
                st.metric("Cloud Grand Total (All Time)", 
                          f"Sales: {neon_total_sales:,} | Purchases: {neon_total_purchases:,}")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Records", f"{len(preview_df):,}")
                with col2:
                    st.metric("Unique Brands", f"{preview_df['brand'].nunique():,}")
                with col3:
                    st.metric("Unique Categories", f"{preview_df['category'].nunique():,}")
                st.dataframe(preview_df.style.format({"sales_qty": "{:,}", "purchase_qty": "{:,}", "mrp": "{:.2f}"}), height=400)
            else:
                st.warning("No data available in the database yet!")
    
    with tab3:
        st.subheader("Visualizations")
        st.write("Explore trends and top performers with interactive charts.")
        start_date = st.date_input("Start Date", datetime.now() - timedelta(days=30), 
                                   help="Pick the start of your data range")
        end_date = st.date_input("End Date", datetime.now(), 
                                 help="Pick the end of your data range")
        viz_refresh_button = st.button("Show Charts", help="Generate charts for your data")
        
        log_placeholder = st.empty()
        log_output = StreamlitLogger(log_placeholder)
        
        if viz_refresh_button or st.session_state.get('results'):
            viz_data = get_visualization_data(log_output, start_date, end_date)
            if viz_data:
                visualizations = create_visualizations(viz_data)
                if visualizations:
                    if "brand" in visualizations and "category" in visualizations:
                        col1, col2 = st.columns(2)
                        with col1:
                            st.plotly_chart(visualizations["brand"], use_container_width=True)
                        with col2:
                            st.plotly_chart(visualizations["category"], use_container_width=True)
                    if "monthly" in visualizations:
                        st.plotly_chart(visualizations["monthly"], use_container_width=True)
                    if "weekly" in visualizations:
                        st.plotly_chart(visualizations["weekly"], use_container_width=True)
                else:
                    st.warning("Could not create visualizations from the available data")
            else:
                st.warning("No data available for visualizations")
    
    with tab4:
        st.subheader("Chatbot")
        st.write("Ask questions about your sales and inventory data and get insights.")
        question_input = st.text_input("Your Question", 
                                       help="Ask something like 'What are the best-selling items this month?'")
        submit_button = st.button("Get Answer", help="Click to get a response")
        response_placeholder = st.empty()
        log_placeholder = st.empty()
        log_output = StreamlitLogger(log_placeholder)
        
        if submit_button and question_input:
            with st.spinner("Processing your question..."):
                response = run_chatbot(question_input, log_output)
            response_placeholder.markdown(response)

if __name__ == "__main__":
    ui()
