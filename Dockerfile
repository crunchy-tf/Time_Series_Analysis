# Start with an official Python base image (consistent with other services)
FROM python:3.10-slim-bookworm

# Set environment variables to make Python print out stuff immediately
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set the working directory in the container
WORKDIR /service

# Install system dependencies needed for building some Python packages
# (e.g., gcc, python3-dev, build-essential for numpy, scipy, statsmodels if they build from source)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    python3-dev \
    # Add any other system libraries if required by pandas/numpy/statsmodels for your specific versions
    # For example, sometimes BLAS/LAPACK libraries like libopenblas-dev might be needed,
    # but often the wheels for numpy/scipy handle this.
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container first to leverage Docker layer caching
COPY ./requirements.txt /service/requirements.txt

# Install Python dependencies
# Use --no-cache-dir to reduce image size
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r /service/requirements.txt

# Copy the rest of your application's code into the container
COPY ./app /service/app
# No model file to copy for this service, as it receives data via API

# Expose the port the app runs on (e.g., 8003 for this service)
EXPOSE 8003

# Command to run the application using Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8003"]