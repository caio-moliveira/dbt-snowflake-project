# Use Python 3.11 as the base image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the entire `streamlit_app` folder contents into the container
COPY . /app

# Install system dependencies for Streamlit and Python libraries
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies for the Streamlit app
RUN pip install --upgrade pip \
    && pip install -r streamlit_requirements.txt

# Expose the default Streamlit port
EXPOSE 8501

# Start the Streamlit application
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.headless=true"]
