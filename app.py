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
DB_RETENTION_DAYS = 7

# Create necessary directories if they don't exist
for directory in [TEMP_STORAGE_DIR, PROCESSED_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# Database connection function
def get_db_connection(db_config):
    """Establish connection to Neon Database"""
    try:
        conn = psycopg2.connect(
            host=db_config["db_host"],
            database=db_config["db_name"],
            user=db_config["db_user"],
            password=db_config["db_password"],
            port=db_config["db_port"],
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        logger.error(f"Connection details: host={db_config['db_host']}, db={db_config['db_name']}, port={db_config['db_port']}, password={db_config['db_password']}")
        return None

# SQLAlchemy Engine for pandas operations
def get_sqlalchemy_engine():
    return create_engine(
        f'postgresql://{st.secrets["db_user"]}:{st.secrets["db_password"]}@{st.secrets["db_host"]}:{st.secrets["db_port"]}/{st.secrets["db_name"]}'
    )

# Function to preprocess the uploaded file
def preprocess_data(file_path, selected_date, log_output):
    """Preprocess the uploaded Excel file according to specified rules"""
    log_output.info(f"Starting preprocessing of file: {os.path.basename(file_path)}")
    
    try:
        # Skip first 9 rows and read the Excel file
        df = pd.read_excel(file_path, skiprows=9)
        log_output.info(f"File loaded. Raw shape: {df.shape}")
        
        # Check if required columns exist
        required_cols = ['Brand', 'Category', 'Size', 'MRP', 'Color', 'SalesQty', 'PurchaseQty']
        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            log_output.error(f"Missing required columns: {missing_cols}")
            return None
        
        # Extract only required columns
        df = df[required_cols].copy()
        
        # Handle missing values
        df.fillna({'SalesQty': 0, 'PurchaseQty': 0}, inplace=True)
        
        # Add date column with user input
        df['date'] = pd.to_datetime(selected_date)
        
        # Compute Week and Month columns
        df['Week'] = df['date'].dt.strftime('%Y-%W')
        df['Month'] = df['date'].dt.strftime('%Y-%m')
        
        # Create a unique identifier for each record
        df['record_id'] = df.apply(
            lambda x: f"{x['Brand']}_{x['Category']}_{x['Size']}_{x['Color']}", 
            axis=1
        )
        
        log_output.info("Checking for duplicates...")
        before_dedup = len(df)
        
        # Find duplicate records based on record_id
        duplicates = df[df.duplicated('record_id', keep=False)].copy()
        unique_records = df[~df.duplicated('record_id', keep='first')].copy()
        
        # Process duplicates if any exist
        if len(duplicates) > 0:
            log_output.info(f"Found {len(duplicates)} duplicate entries to process")
            # Group duplicates and sum quantities
            dup_grouped = duplicates.groupby('record_id').agg({
                'Brand': 'first',
                'Category': 'first',
                'Size': 'first',
                'MRP': 'first',
                'Color': 'first',
                'SalesQty': 'sum',
                'PurchaseQty': 'sum',
                'date': 'first',
                'Week': 'first',
                'Month': 'first'
            }).reset_index()
            
            # Combine unique records with processed duplicates
            final_df = pd.concat([unique_records.drop('record_id', axis=1), 
                                  dup_grouped.drop('record_id', axis=1)], 
                                 ignore_index=True)
        else:
            final_df = unique_records.drop('record_id', axis=1)
        
        after_dedup = len(final_df)
        log_output.info(f"Removed {before_dedup - after_dedup} duplicates.")
        
        # Clean up data types
        final_df['SalesQty'] = final_df['SalesQty'].astype(int)
        final_df['PurchaseQty'] = final_df['PurchaseQty'].astype(int)
        final_df['MRP'] = final_df['MRP'].astype(float)
        
        # Sort by date with newest first
        final_df = final_df.sort_values('date', ascending=False)
        
        log_output.info(f"Preprocessing complete. Final shape: {final_df.shape}")
        return final_df
    
    except Exception as e:
        log_output.error(f"Error during preprocessing: {str(e)}")
        return None

# Function to save preprocessed file
def save_preprocessed_file(df, selected_date, log_output):
    """Save the preprocessed dataframe as Excel file"""
    try:
        date_str = selected_date.strftime('%Y%m%d')
        file_name = f"salesninventory_{date_str}.xlsx"
        file_path = os.path.join(PROCESSED_DIR, file_name)
        
        df.to_excel(file_path, index=False)
        log_output.info(f"Preprocessed file saved: {file_name}")
        
        # Update master summary file
        update_master_summary(df, log_output)
        
        return file_path
    except Exception as e:
        log_output.error(f"Error saving preprocessed file: {str(e)}")
        return None

# Function to update master summary
def update_master_summary(new_df, log_output):
    """Update the master summary Excel file with new data"""
    master_file = os.path.join(PROCESSED_DIR, MASTER_SUMMARY_FILE)
    
    try:
        if os.path.exists(master_file):
            master_df = pd.read_excel(master_file)
            
            # Identify records to update (same Brand, Category, Size, Color)
            merge_cols = ['Brand', 'Category', 'Size', 'Color', 'Month']
            
            # Create a unique identifier for each record
            master_df['record_id'] = master_df.apply(
                lambda x: f"{x['Brand']}_{x['Category']}_{x['Size']}_{x['Color']}_{x['Month']}", 
                axis=1
            )
            new_df['record_id'] = new_df.apply(
                lambda x: f"{x['Brand']}_{x['Category']}_{x['Size']}_{x['Color']}_{x['Month']}", 
                axis=1
            )
            
            # Find records to update (in both dataframes)
            records_to_update = new_df[new_df['record_id'].isin(master_df['record_id'])].copy()
            
            # Find records to add (only in new_df)
            records_to_add = new_df[~new_df['record_id'].isin(master_df['record_id'])].copy()
            
            # Update existing records
            for _, row in records_to_update.iterrows():
                master_df.loc[master_df['record_id'] == row['record_id'], 'SalesQty'] = row['SalesQty']
                master_df.loc[master_df['record_id'] == row['record_id'], 'PurchaseQty'] = row['PurchaseQty']
                master_df.loc[master_df['record_id'] == row['record_id'], 'date'] = row['date']
                master_df.loc[master_df['record_id'] == row['record_id'], 'MRP'] = row['MRP']
            
            # Add new records
            if len(records_to_add) > 0:
                records_to_add = records_to_add.drop('record_id', axis=1)
                master_df = master_df.drop('record_id', axis=1)
                master_df = pd.concat([master_df, records_to_add], ignore_index=True)
            else:
                master_df = master_df.drop('record_id', axis=1)
            
            # Sort by date with newest first
            master_df = master_df.sort_values('date', ascending=False)
            
            log_output.info(f"Master summary updated. New size: {len(master_df)}")
        else:
            master_df = new_df.copy()
            # Sort by date with newest first
            master_df = master_df.sort_values('date', ascending=False)
            log_output.info("Created new master summary file")
        
        # Save the updated master summary
        master_df.to_excel(master_file, index=False)
        
        return True
    except Exception as e:
        log_output.error(f"Error updating master summary: {str(e)}")
        return False
    
# Function to enforce retention policy
def enforce_retention_policy(log_output):
    """Delete files older than retention period (7 days)"""
    try:
        files = glob.glob(os.path.join(PROCESSED_DIR, "salesninventory_*.xlsx"))
        files.sort()
        
        # Keep only the latest 7 files
        if len(files) > DB_RETENTION_DAYS:
            files_to_delete = files[:-DB_RETENTION_DAYS]
            for file in files_to_delete:
                os.remove(file)
                log_output.info(f"Deleted old file: {os.path.basename(file)}")
        
        return True
    except Exception as e:
        log_output.error(f"Error enforcing retention policy: {str(e)}")
        return False

# Function to upload data to Neon database
# Function to upload data to Neon database
# Function to upload data to Neon database
def upload_to_database(df, log_output):
    """Upload preprocessed data to Neon database with optimizations"""
    # Create db_config dictionary from Streamlit secrets
    db_config = {
        "db_host": st.secrets["db_host"],
        "db_name": st.secrets["db_name"],
        "db_user": st.secrets["db_user"],
        "db_password": st.secrets["db_password"],
        "db_port": st.secrets["db_port"]
    }
    
    conn = get_db_connection(db_config)
    if not conn:
        log_output.error("Failed to connect to database")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Create indexes for faster lookups
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_sales_lookup 
        ON sales_data (brand, category, size, color, month)
        """)
        
        conn.commit()
        
        # Check if table is empty for first-time upload
        cursor.execute("SELECT COUNT(*) FROM sales_data")
        count = cursor.fetchone()[0]
        
        if count == 0:
            log_output.info("First upload - inserting all records")
            # For first upload, use COPY for better performance
            upload_using_copy(df, log_output)
            
            # Get updated counts for summary
            new_records = len(df)
            updated_records = 0
            
            log_output.info(f"Initial upload complete. Added {new_records} records.")
        else:
            # For subsequent uploads, use batch processing and merging
            new_records, updated_records = merge_data_with_existing(df, db_config, log_output)
            log_output.info(f"Upload complete. Added {new_records} new records, updated {updated_records} records.")
        
        conn.close()
        return {"new": new_records, "updated": updated_records}
    
    except Exception as e:
        log_output.error(f"Database upload error: {str(e)}")
        if conn:
            conn.close()
        return False

# Use COPY command for initial large upload
# Use COPY command for initial large upload
def upload_using_copy(df, log_output):
    """Use SQLAlchemy for efficient bulk data upload with proper handling of special characters"""
    try:
        engine = get_sqlalchemy_engine()
        
        # Prepare dataframe for database
        db_df = df[['Brand', 'Category', 'Size', 'MRP', 'Color', 'Week', 'Month', 'SalesQty', 'PurchaseQty']].copy()
        db_df.columns = ['brand', 'category', 'size', 'mrp', 'color', 'week', 'month', 'sales_qty', 'purchase_qty']
        
        # Create a temporary table
        with engine.connect() as conn:
            # First drop the temp table if it exists from a previous run
            conn.execute(text("DROP TABLE IF EXISTS sales_data_temp"))
            
            # Create the temporary table
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
                    purchase_qty INTEGER
                )
            """))
        
        # Load data into temporary table using SQLAlchemy
        db_df.to_sql('sales_data_temp', engine, if_exists='append', index=False)
        
        # Now perform a direct INSERT from temp to the main table
        with engine.begin() as conn:
            # Get total count of records to be inserted
            result = conn.execute(text("SELECT COUNT(*) FROM sales_data_temp"))
            total_count = result.scalar()
            
            # Direct insert all records from temp to main table
            conn.execute(text("""
                INSERT INTO sales_data 
                (brand, category, size, mrp, color, week, month, sales_qty, purchase_qty)
                SELECT brand, category, size, mrp, color, week, month, sales_qty, purchase_qty
                FROM sales_data_temp
            """))
            
            # Drop temporary table
            conn.execute(text("DROP TABLE IF EXISTS sales_data_temp"))
        
        log_output.info(f"Successfully inserted all {total_count} records into sales_data table")
        return {"new": total_count, "updated": 0}  # For first upload, all records are new
    
    except Exception as e:
        log_output.error(f"Error during database upload: {str(e)}")
        try:
            # Make sure to clean up temp table even on error
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sales_data_temp"))
        except:
            pass
        return False
    
# Function to merge data with existing records
def merge_data_with_existing(df, db_config, log_output):
    """Merge new data with existing data in the database"""
    conn = get_db_connection(db_config)
    if not conn:
        log_output.error("Failed to connect to database")
        return 0, 0
    
    try:
        cursor = conn.cursor()
        
        # Create a temporary table for new data
        cursor.execute("""
        CREATE TEMPORARY TABLE temp_sales_data (
            brand VARCHAR(100),
            category VARCHAR(100),
            size VARCHAR(50),
            mrp FLOAT,
            color VARCHAR(50),
            week VARCHAR(10),
            month VARCHAR(10),
            sales_qty INTEGER,
            purchase_qty INTEGER
        )
        """)
        
        # Prepare data for insertion into temp table
        temp_data = []
        for _, row in df.iterrows():
            temp_data.append((
                row['Brand'], row['Category'], row['Size'], float(row['MRP']), 
                row['Color'], row['Week'], row['Month'], 
                int(row['SalesQty']), int(row['PurchaseQty'])
            ))
        
        # Batch insert into temp table
        psycopg2.extras.execute_batch(
            cursor,
            """
            INSERT INTO temp_sales_data 
            (brand, category, size, mrp, color, week, month, sales_qty, purchase_qty)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            temp_data,
            page_size=1000
        )
        
        # Count records for update (existing in both tables)
        cursor.execute("""
        SELECT COUNT(*) FROM temp_sales_data t
        JOIN sales_data s ON 
            t.brand = s.brand AND 
            t.category = s.category AND 
            t.size = s.size AND 
            t.color = s.color AND
            t.month = s.month
        """)
        to_update = cursor.fetchone()[0]
        
        # Update existing records - REPLACE quantities instead of adding them
        cursor.execute("""
        UPDATE sales_data s
        SET 
            sales_qty = t.sales_qty,
            purchase_qty = t.purchase_qty,
            created_at = CURRENT_TIMESTAMP
        FROM temp_sales_data t
        WHERE 
            t.brand = s.brand AND 
            t.category = s.category AND 
            t.size = s.size AND 
            t.color = s.color AND
            t.month = s.month
        """)
        
        # Insert new records (not in sales_data)
        cursor.execute("""
        WITH inserted AS (
            INSERT INTO sales_data 
            (brand, category, size, mrp, color, week, month, sales_qty, purchase_qty)
            SELECT t.brand, t.category, t.size, t.mrp, t.color, t.week, t.month, t.sales_qty, t.purchase_qty
            FROM temp_sales_data t
            WHERE NOT EXISTS (
                SELECT 1 FROM sales_data s
                WHERE 
                    t.brand = s.brand AND 
                    t.category = s.category AND 
                    t.size = s.size AND 
                    t.color = s.color AND
                    t.month = s.month
            )
            RETURNING *
        )
        SELECT COUNT(*) FROM inserted
        """)
        new_records = cursor.fetchone()[0]
        
        # Drop the temporary table
        cursor.execute("DROP TABLE IF EXISTS temp_sales_data")
        
        # Count new records inserted
        new_records = cursor.rowcount if cursor.rowcount > 0 else 0
        
        # Commit and close connection
        conn.commit()
        conn.close()
        
        log_output.info(f"Database update complete: {to_update} records updated, {new_records} new records inserted")
        return new_records, to_update
    
    except Exception as e:
        log_output.error(f"Error during merge operation: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()
        return 0, 0

# Function to get data from database for visualization
def get_database_preview(log_output):
    """Get a preview of the data in the database"""
    try:
        engine = get_sqlalchemy_engine()
        query = "SELECT * FROM sales_data ORDER BY created_at DESC LIMIT 1000"
        df = pd.read_sql(query, engine)
        log_output.info(f"Retrieved {len(df)} records for preview")
        return df
    except Exception as e:
        log_output.error(f"Error getting database preview: {str(e)}")
        return pd.DataFrame()

# Function to get aggregated data for visualizations
def get_visualization_data(log_output):
    """Get aggregated data for visualizations"""
    try:
        engine = get_sqlalchemy_engine()
        
        # Brand-wise sales and purchases
        brand_query = """
        SELECT brand, SUM(sales_qty) as total_sales, SUM(purchase_qty) as total_purchases
        FROM sales_data
        GROUP BY brand
        ORDER BY total_sales DESC
        LIMIT 10
        """
        brand_df = pd.read_sql(brand_query, engine)
        
        # Category-wise sales and purchases
        category_query = """
        SELECT category, SUM(sales_qty) as total_sales, SUM(purchase_qty) as total_purchases
        FROM sales_data
        GROUP BY category
        ORDER BY total_sales DESC
        LIMIT 10
        """
        category_df = pd.read_sql(category_query, engine)
        
        # Monthly trends
        monthly_query = """
        SELECT month, SUM(sales_qty) as total_sales, SUM(purchase_qty) as total_purchases
        FROM sales_data
        GROUP BY month
        ORDER BY month
        """
        monthly_df = pd.read_sql(monthly_query, engine)
        
        # Weekly trends
        weekly_query = """
        SELECT week, SUM(sales_qty) as total_sales, SUM(purchase_qty) as total_purchases
        FROM sales_data
        GROUP BY week
        ORDER BY week
        """
        weekly_df = pd.read_sql(weekly_query, engine)
        
        log_output.info("Retrieved aggregated data for visualizations")
        
        return {
            "brand": brand_df,
            "category": category_df,
            "monthly": monthly_df,
            "weekly": weekly_df
        }
    except Exception as e:
        log_output.error(f"Error getting visualization data: {str(e)}")
        return {}

# Create visualizations
def create_visualizations(data):
    """Create visualizations using Plotly"""
    visualizations = {}
    
    if not data:
        return visualizations
    
    # Brand-wise visualization
    if "brand" in data and not data["brand"].empty:
        brand_fig = px.bar(
            data["brand"], 
            x="brand", 
            y=["total_sales", "total_purchases"],
            title="Top 10 Brands by Sales and Purchases",
            barmode="group"
        )
        visualizations["brand"] = brand_fig
    
    # Category-wise visualization
    if "category" in data and not data["category"].empty:
        category_fig = px.bar(
            data["category"], 
            x="category", 
            y=["total_sales", "total_purchases"],
            title="Top 10 Categories by Sales and Purchases",
            barmode="group"
        )
        visualizations["category"] = category_fig
    
    # Monthly trends
    if "monthly" in data and not data["monthly"].empty:
        monthly_fig = px.line(
            data["monthly"], 
            x="month", 
            y=["total_sales", "total_purchases"],
            title="Monthly Sales and Purchase Trends"
        )
        visualizations["monthly"] = monthly_fig
    
    # Weekly trends
    if "weekly" in data and not data["weekly"].empty:
        weekly_fig = px.line(
            data["weekly"], 
            x="week", 
            y=["total_sales", "total_purchases"],
            title="Weekly Sales and Purchase Trends"
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
        log_text = "\n".join(self.logs[-20:])  # Show last 20 logs
        self.placeholder.code(log_text, language="bash")

# Streamlit UI
def ui():
    st.title("Sales & Inventory Data Pipeline")
    
    # Create a sidebar
    st.sidebar.header("Pipeline Controls")
    
    # File upload
    uploaded_file = st.sidebar.file_uploader("Upload XLSX File", type=["xlsx"])
    
    # Date selection
    selected_date = st.sidebar.date_input(
        "Select Date for the Data",
        datetime.now()
    )
    
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["Pipeline", "Database Preview", "Visualizations"])
    
    with tab1:
        # Split into columns
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Log display area
            st.subheader("Processing Logs")
            log_placeholder = st.empty()
            log_output = StreamlitLogger(log_placeholder)
            
            # Progress bar
            progress_bar = st.progress(0)
            
            # Initialize session state for results if not exist
            if 'results' not in st.session_state:
                st.session_state.results = None
            
            # Process button
            process_button = st.button("Start Processing")
            
            if process_button and uploaded_file is not None:
                # Reset any previous results
                st.session_state.results = None
                
                # Update progress
                progress_bar.progress(10)
                log_output.info("Starting data pipeline process")
                
                # Save uploaded file temporarily
                temp_file_path = os.path.join(TEMP_STORAGE_DIR, uploaded_file.name)
                with open(temp_file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                log_output.info(f"File saved temporarily: {uploaded_file.name}")
                progress_bar.progress(20)
                
                # Preprocess data
                df = preprocess_data(temp_file_path, selected_date, log_output)
                if df is not None:
                    progress_bar.progress(40)
                    
                    # Save preprocessed file
                    preprocessed_path = save_preprocessed_file(df, selected_date, log_output)
                    if preprocessed_path:
                        progress_bar.progress(60)
                        
                        # Enforce retention policy
                        enforce_retention_policy(log_output)
                        progress_bar.progress(70)
                        
                        # Upload to database
                        results = upload_to_database(df, log_output)
                        if results:
                            progress_bar.progress(90)
                            
                            # Store results in session state
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
                
                # Clean up temporary file
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
        
        with col2:
            st.subheader("Process Summary")
            
            if st.session_state.results:
                results = st.session_state.results
                st.success("✅ Processing Completed")
                
                st.write(f"**Date:** {results['date']}")
                st.write(f"**Total Records Processed:** {results['total_records']}")
                st.write(f"**New Records Added:** {results['new_records']}")
                st.write(f"**Records Updated:** {results['updated_records']}")
                
                # Add a download button for the processed file
                date_str = selected_date.strftime('%Y%m%d')
                file_name = f"salesninventory_{date_str}.xlsx"
                file_path = os.path.join(PROCESSED_DIR, file_name)
                
                if os.path.exists(file_path):
                    with open(file_path, "rb") as f:
                        st.download_button(
                            label="Download Processed File",
                            data=f,
                            file_name=file_name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
            else:
                st.info("Upload a file and click 'Start Processing' to begin")
    
    with tab2:
        st.subheader("Database Preview")
        refresh_button = st.button("Refresh Database Preview")
        
        # Initialize log output if not in this tab
        if 'log_output' not in locals():
            log_placeholder = st.empty()
            log_output = StreamlitLogger(log_placeholder)
        
        if refresh_button or st.session_state.results:
            # Get data preview
            preview_df = get_database_preview(log_output)
            
            if not preview_df.empty:
                # Display database stats
                total_records = len(preview_df)
                unique_brands = preview_df['brand'].nunique()
                unique_categories = preview_df['category'].nunique()
                
                # Create columns for stats
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Records", f"{total_records:,}")
                with col2:
                    st.metric("Unique Brands", f"{unique_brands:,}")
                with col3:
                    st.metric("Unique Categories", f"{unique_categories:,}")
                
                # Display the dataframe
                st.dataframe(preview_df)
            else:
                st.warning("No data available in the database")
    
    with tab3:
        st.subheader("Data Visualizations")
        viz_refresh_button = st.button("Refresh Visualizations")
        
        # Initialize log output if not in this tab
        if 'log_output' not in locals():
            log_placeholder = st.empty()
            log_output = StreamlitLogger(log_placeholder)
        
        if viz_refresh_button or st.session_state.results:
            # Get visualization data
            viz_data = get_visualization_data(log_output)
            
            if viz_data:
                # Create visualizations
                visualizations = create_visualizations(viz_data)
                
                # Display visualizations
                if visualizations:
                    # Display brand and category side by side
                    if "brand" in visualizations and "category" in visualizations:
                        col1, col2 = st.columns(2)
                        with col1:
                            st.plotly_chart(visualizations["brand"], use_container_width=True)
                        with col2:
                            st.plotly_chart(visualizations["category"], use_container_width=True)
                    
                    # Display time series charts
                    if "monthly" in visualizations:
                        st.plotly_chart(visualizations["monthly"], use_container_width=True)
                    
                    if "weekly" in visualizations:
                        st.plotly_chart(visualizations["weekly"], use_container_width=True)
                else:
                    st.warning("Could not create visualizations from the available data")
            else:
                st.warning("No data available for visualizations")

# Entry point for Streamlit app
if __name__ == "__main__":
    ui()