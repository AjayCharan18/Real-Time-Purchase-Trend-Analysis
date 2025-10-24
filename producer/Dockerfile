FROM python:3.10-slim

WORKDIR /app

# Install Python dependencies (pin NumPy before Pandas to ensure ABI compatibility)
RUN pip install --no-cache-dir \
    numpy==1.26.4 \
    pandas==2.1.4 \
    kafka-python==2.0.2

# Copy application code
COPY . /app/

# Run producer
CMD ["python", "kafka_producer.py"]
