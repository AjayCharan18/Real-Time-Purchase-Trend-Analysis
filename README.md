# Real-Time Purchase Trend Analysis

##  **Project Overview**
**Problem Statement**: Retailers fail to respond quickly to sudden changes in buying trends, leading to missed revenue opportunities.

**Objective**: Stream purchase transactions (Instacart dataset) via Kafka, store in Hadoop, and generate live dashboards of product popularity.

##  **System Architecture**
### Architecture

Instacart Dataset (optional) → Kafka Producer → Kafka → Spark Streaming → HDFS → Flask Dashboard

### **Key Components:**
- **Data Source**: Instacart purchase transactions (realistic retail data)
- **Message Broker**: Apache Kafka (real-time streaming)
- **Stream Processing**: Apache Spark Streaming (real-time analytics)
- **Storage**: Hadoop HDFS (distributed data storage)
- **Visualization**: Flask Dashboard with live charts and business insights
- **Producer**: Simulates real-time transaction generation

## Quick Start

### Prerequisites
- Docker Desktop with WSL2 (Windows) or Docker Engine (Linux/Mac)
- 8GB+ RAM recommended
- Internet connection for downloading dependencies

### Setup and Run

1. **Clone and navigate to project**
```bash
git clone <repository-url>
cd PURCHASE-TREND-ANALYSIS
```

2. **Create required directories**
```bash
mkdir -p logs/producer logs/spark logs/dashboard caches/ivy2 caches/m2
```

3. **(Optional) Add Instacart CSVs for real data mode**
   Place the Instacart dataset CSVs inside `data/` before starting containers:

   - `data/orders.csv`
   - `data/products.csv`
   - `data/order_products__prior.csv`
   - `data/departments.csv`

   By default the producer runs in `synthetic` mode (`PRODUCER_DATA_MODE=synthetic`).
   To stream the real dataset, set `PRODUCER_DATA_MODE=auto` (or `real`) in
   `docker-compose.yml` and ensure the CSV files exist at the paths above.

4. **Start infrastructure services**
```bash
docker compose up -d zookeeper kafka hadoop-namenode hadoop-datanode spark-master spark-worker
```

5. **Verify services are healthy**
```bash
docker compose ps
```

6. **Create Kafka topic**
```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic purchase-transactions --partitions 1 --replication-factor 1
```

7. **Create HDFS directories**
```bash
docker compose exec hadoop-namenode hdfs dfs -mkdir -p /purchase-analytics/raw
docker compose exec hadoop-namenode hdfs dfs -mkdir -p /purchase-analytics/aggregated/by_product
docker compose exec hadoop-namenode hdfs dfs -mkdir -p /purchase-analytics/aggregated/by_department
docker compose exec hadoop-namenode hdfs dfs -mkdir -p /purchase-analytics/aggregated/by_hour
```

8. **Build and start Spark streaming**
```bash
docker compose build --no-cache spark-streaming
docker compose up -d spark-streaming
```

9. **Start data producer**
```bash
docker compose up -d producer
```

10. **Monitor logs**
```bash
# Producer logs
docker compose logs -f producer

# Spark streaming logs
docker compose logs -f spark-streaming
```

11. **Verify data flow (after ~1 minute)**
```bash
docker compose exec hadoop-namenode hdfs dfs -ls /purchase-analytics/raw
docker compose exec hadoop-namenode hdfs dfs -ls /purchase-analytics/aggregated/by_product
```

12. **Start dashboard**
```bash
docker compose up -d dashboard
```

13. **Access dashboard**
Open http://localhost:5000 in your browser

## Configuration

### Environment Variables

#### Producer Service
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: kafka:29092)
- `KAFKA_TOPIC`: Topic name (default: purchase-transactions)
- `PRODUCER_DATA_MODE`: Data mode - "synthetic" or "auto" (default: synthetic)
- `PRODUCER_RATE`: Messages per second (default: 100)
- `DATA_PATH`: Path to Instacart CSV files (default: /app/data)

#### Spark Streaming Service
- `SPARK_MASTER_URL`: Spark master URL (default: spark://spark-master:7077)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address
- `KAFKA_TOPIC`: Topic to consume from
- `HDFS_NAMENODE`: HDFS namenode URL (default: hdfs://hadoop-namenode:8020)
- `CHECKPOINT_DIR`: Spark checkpoint directory (default: /tmp/spark-checkpoints)
- `RESET_CHECKPOINTS_ON_START`: When `true` (default), purges local Spark checkpoint directories at container startup to avoid incomplete log errors during development

#### Dashboard Service
- `FLASK_ENV`: Flask environment (default: development)
- `HDFS_NAMENODE`: HDFS namenode URL
- `HDFS_DATA_PATH`: Path to aggregated data (default: /purchase-analytics/aggregated)

## Data Pipeline

### 1. Data Generation
- **Synthetic Mode**: Generates realistic transaction data immediately
- **Real Data Mode**: Loads actual Instacart CSV files (orders.csv, products.csv, etc.)

### 2. Streaming Processing
Spark Streaming processes data in 30-second micro-batches and creates:
- **Raw Data**: Unprocessed transactions in Parquet format
- **Product Aggregations**: Sales by product with metrics
- **Department Aggregations**: Sales by department
- **Hourly Aggregations**: Time-based sales patterns

### 3. Storage
All data stored in HDFS as Parquet files:
```
/purchase-analytics/
├── raw/                    # Raw transaction data
├── aggregated/
│   ├── by_product/        # Product-level aggregations
│   ├── by_department/     # Department-level aggregations
│   └── by_hour/          # Hourly aggregations
```

### 4. Visualization
Flask dashboard provides:
- Real-time transaction volume
- Top products by sales
- Department revenue distribution
- Hourly sales trends
- Alerting, PDF export, and Excel export tools

## Troubleshooting

### Common Issues

1. **Spark streaming not starting**
```bash
# Check logs
docker compose logs spark-streaming

# Rebuild if version mismatch
docker compose build --no-cache spark-streaming
docker compose up -d --force-recreate spark-streaming
```

2. **Producer connection errors**
```bash
# Verify Kafka is healthy
docker compose logs kafka

# Restart producer
docker compose up -d --force-recreate producer
```

3. **HDFS directories not found**
```bash
# Create missing directories
docker compose exec hadoop-namenode hdfs dfs -mkdir -p /purchase-analytics/raw
docker compose exec hadoop-namenode hdfs dfs -mkdir -p /purchase-analytics/aggregated/by_product
docker compose exec hadoop-namenode hdfs dfs -mkdir -p /purchase-analytics/aggregated/by_department
docker compose exec hadoop-namenode hdfs dfs -mkdir -p /purchase-analytics/aggregated/by_hour
```

4. **Spark streaming fails with incomplete checkpoint logs**
```bash
# Remove local checkpoint directories and restart streaming container
docker compose run --rm spark-streaming bash -lc 'rm -rf /tmp/spark-checkpoints'
docker compose up -d spark-streaming
```

4. **Dashboard not showing data**
```bash
# Verify HDFS has data
docker compose exec hadoop-namenode hdfs dfs -ls /purchase-analytics/aggregated/by_product

# Check dashboard logs
docker compose logs dashboard
```

### Performance Tuning

1. **Increase producer rate**
```bash
docker compose up -d --scale producer=2
```

2. **Adjust Spark resources**
```bash
docker compose exec spark-master ./bin/spark-class org.apache.spark.deploy.Client kill spark://spark-master:7077 <driver-id>
```

3. **Optimize HDFS**
- Use larger block size for big workloads
- Monitor Namenode UI for usage patterns

##  Dashboard Features

- Live transaction feed with cumulative stats
- Top products chart with trend indicators
- Department revenue distribution
- Hourly transaction trends with revenue overlay
- Real-time alerts for business insights
- Export options: PDF reports and Excel sheets

## Testing

Automated tests have not been added yet. Contributions are welcome—add unit tests and
integration tests (e.g., Spark job validation) under a future `tests/` directory and
run them with `pytest` once available.

##  Support

For questions or contributions, open an issue or submit a pull request.
