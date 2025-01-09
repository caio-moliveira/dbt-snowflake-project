# Use the official dbt Docker image
FROM ghcr.io/dbt-labs/dbt-snowflake:1.4.4

# Set the working directory in the container
WORKDIR /usr/app

# Copy the dbt project files into the container
COPY . /usr/app

# Install Python dependencies (if any)
# If your project uses additional Python dependencies, list them in requirements.txt
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


# Set the entry point for dbt commands
ENTRYPOINT ["dbt"]
