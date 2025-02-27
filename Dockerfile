# Use official Python runtime as base image
FROM python:3.9-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies for psycopg2 and matplotlib
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    python3-dev \
    libjpeg-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and secrets
COPY app.py .
COPY .streamlit/ .streamlit/

# Create directories for temp and processed data inside the container
RUN mkdir -p temp_storage processed_data

# Expose Streamlit port
EXPOSE 8501

# Command to run Streamlit app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]