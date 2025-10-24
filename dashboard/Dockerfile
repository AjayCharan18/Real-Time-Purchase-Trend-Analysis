FROM python:3.10-slim

WORKDIR /app

# Install Java and required system packages
RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

RUN pip install --no-cache-dir \
    flask==3.0.0 \
    flask-socketio==5.3.4 \
    flask-cors==4.0.0 \
    pandas==2.1.0 \
    pyspark==3.5.2 \
    python-socketio==5.9.0 \
    eventlet==0.33.3 \
    requests==2.31.0 \
    reportlab==4.0.9 \
    openpyxl==3.1.2 \
    matplotlib==3.8.0 \
    Pillow==10.0.0

COPY . /app/

EXPOSE 5000

CMD ["python", "app.py"]
