# Kafka Producer - Simulates Real-Time Purchase Transactions
import os
import json
import time
import random
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/producer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class InstacartDataSimulator:
    """Simulates real-time purchase transactions from Instacart dataset"""
    
    def __init__(self, data_path, mode: str = "auto"):
        self.data_path = data_path
        self.mode = (mode or "auto").lower()
        
        # Placeholders for datasets
        self.orders_df = None
        self.products_df = None
        self.order_products_df = None
        self.departments_df = None
        self.merged_df = None
        self.synthetic_products = []
        self.load_datasets()
    
    def load_datasets(self):
        """Load Instacart datasets from CSV files"""
        try:
            logger.info(f"Loading datasets from {self.data_path}")
            # Fast path: synthetic mode skips heavy CSV load/merge
            if self.mode == "synthetic":
                logger.info("PRODUCER_DATA_MODE=synthetic: Skipping CSV load. Using synthetic data.")
                self.generate_synthetic_data()
                return
            # Lazy import pandas and fall back if unavailable or incompatible
            try:
                import pandas as pd  # noqa: F401
            except Exception as ie:
                raise RuntimeError(f"Pandas import failed: {ie}")

            # Try to load real Instacart data (matching provided filenames)
            orders_path = f"{self.data_path}/orders.csv"
            products_path = f"{self.data_path}/products.csv"
            order_products_path = f"{self.data_path}/order_products__prior.csv"
            departments_path = f"{self.data_path}/departments.csv"

            self.orders_df = pd.read_csv(orders_path)
            self.products_df = pd.read_csv(products_path)
            self.order_products_df = pd.read_csv(order_products_path)
            self.departments_df = pd.read_csv(departments_path)

            # Merge datasets for enriched transaction data
            self.merged_df = self.order_products_df.merge(
                self.products_df, on='product_id'
            ).merge(
                self.orders_df, on='order_id'
            ).merge(
                self.departments_df, on='department_id'
            )
            
            logger.info(f"Successfully loaded {len(self.merged_df)} transaction records from real data")
            
        except Exception as e:
            logger.warning(f"Could not load Instacart data: {str(e)}")
            logger.info("Generating synthetic data instead...")
            self.generate_synthetic_data()
    
    def generate_synthetic_data(self):
        """Generate synthetic purchase data if real dataset is unavailable"""
        logger.info("Generating synthetic Instacart-like data...")
        
        product_names = [
            "Organic Bananas", "Organic Strawberries", "Organic Baby Spinach",
            "Organic Avocado", "Organic Hass Avocado", "Large Lemon",
            "Organic Whole Milk", "Organic Raspberries", "Organic Yellow Onion",
            "Organic Garlic", "Organic Zucchini", "Organic Blueberries",
            "Cucumber Kirby", "Organic Fuji Apple", "Organic Lemon",
            "Organic Grape Tomatoes", "Organic Ginger Root", "Organic Cilantro",
            "Organic Celery", "Organic Lime", "Seedless Red Grapes",
            "Organic Baby Carrots", "Organic Strawberry", "Honeycrisp Apple",
            "Organic Cucumber", "Organic Roma Tomato", "Organic Yellow Banana",
            "Organic Red Onion", "Organic Broccoli Crown", "Organic Kale"
        ]
        
        departments = [
            'produce', 'dairy eggs', 'snacks', 'beverages', 'frozen',
            'bakery', 'meat seafood', 'pantry', 'deli', 'household'
        ]
        
        aisles = [
            'fresh fruits', 'fresh vegetables', 'packaged vegetables fruits',
            'yogurt', 'milk', 'eggs', 'chips pretzels', 'cookies cakes',
            'water seltzer sparkling water', 'soy lactosefree', 'juice nectars',
            'frozen produce', 'frozen meals', 'bread', 'cheese', 'fresh dips tapenades'
        ]
        
        products = []
        for i, name in enumerate(product_names):
            products.append({
                "id": i + 1,
                "name": name,
                "aisle": random.choice(aisles),
                "department": random.choice(departments)
            })
        
        self.synthetic_products = products
        logger.info(f"Generated {len(products)} synthetic products")
    
    def generate_transaction(self):
        """Generate a single purchase transaction"""
        try:
            if self.merged_df is not None and len(self.merged_df) > 0:
                # Use real data
                row = self.merged_df.sample(n=1).iloc[0]
                
                # Generate random timestamp within the last 24 hours for better hourly distribution
                hours_ago = random.randint(0, 23)
                minutes_ago = random.randint(0, 59)
                random_time = datetime.now() - timedelta(hours=hours_ago, minutes=minutes_ago)
                
                transaction = {
                    "transaction_id": f"TXN_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
                    "timestamp": random_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "order_id": int(row['order_id']),
                    "product_id": int(row['product_id']),
                    "product_name": row['product_name'],
                    "department": row['department'],
                    "aisle_id": (lambda v: int(v) if (v is not None and v == v) else random.randint(1, 134))(row.get('aisle_id')),
                    "quantity": int(row.get('add_to_cart_order', random.randint(1, 5))),
                    "reordered": int(row.get('reordered', 0)),
                    "user_id": int(row['user_id']),
                    "order_number": int(row['order_number']),
                    "order_dow": int(row['order_dow']),
                    "order_hour": int(row['order_hour_of_day']),
                    "days_since_prior_order": (lambda v: float(v) if (isinstance(v, (int, float)) and v == v) else 0.0)(row.get('days_since_prior_order')),
                    "price": round(random.uniform(1.99, 49.99), 2),
                    "discount": round(random.uniform(0, 0.3), 2)
                }
            else:
                # Use synthetic data
                product = random.choice(self.synthetic_products)
                
                # Generate random timestamp within the last 24 hours for better hourly distribution
                hours_ago = random.randint(0, 23)
                minutes_ago = random.randint(0, 59)
                random_time = datetime.now() - timedelta(hours=hours_ago, minutes=minutes_ago)
                
                transaction = {
                    "transaction_id": f"TXN_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
                    "timestamp": random_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "order_id": random.randint(1000000, 9999999),
                    "product_id": product['id'],
                    "product_name": product['name'],
                    "department": product['department'],
                    "aisle_id": random.randint(1, 134),
                    "quantity": random.randint(1, 5),
                    "reordered": random.randint(0, 1),
                    "user_id": random.randint(1, 100000),
                    "order_number": random.randint(1, 100),
                    "order_dow": random.randint(0, 6),
                    "order_hour": random.randint(0, 23),
                    "days_since_prior_order": round(random.uniform(0, 30), 1),
                    "price": round(random.uniform(1.99, 49.99), 2),
                    "discount": round(random.uniform(0, 0.3), 2)
                }
            
            return transaction
            
        except Exception as e:
            logger.error(f"Error generating transaction: {str(e)}")
            return None


class PurchaseTransactionProducer:
    """Kafka Producer for streaming purchase transactions"""
    
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.connect()
    
    def connect(self):
        """Establish connection to Kafka"""
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                    compression_type='gzip',
                    linger_ms=10,
                    batch_size=16384
                )
                logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
                return
            except KafkaError as e:
                retry_count += 1
                logger.warning(f"Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {str(e)}")
                time.sleep(5)
        
        raise Exception("Could not connect to Kafka after multiple retries")
    
    def send_transaction(self, transaction):
        """Send a single transaction to Kafka"""
        try:
            if transaction:
                key = str(transaction['product_id'])
                
                future = self.producer.send(
                    self.topic,
                    key=key,
                    value=transaction
                )
                
                future.add_callback(self.on_send_success)
                future.add_errback(self.on_send_error)
                
                return future
        except Exception as e:
            logger.error(f"Error sending transaction: {str(e)}")
    
    def on_send_success(self, record_metadata):
        """Callback for successful send"""
        pass
    
    def on_send_error(self, ex):
        """Callback for failed send"""
        logger.error(f"Error sending message: {str(ex)}")
    
    def flush(self):
        """Flush pending messages"""
        if self.producer:
            self.producer.flush()
    
    def close(self):
        """Close producer connection"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")


def main():
    """Main execution function"""
    
    # Configuration from environment variables
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'purchase-transactions')
    data_path = os.getenv('DATA_PATH', './data')
    data_mode = os.getenv('PRODUCER_DATA_MODE', 'auto')
    producer_rate = int(os.getenv('PRODUCER_RATE', '100'))
    
    logger.info("=== Starting Kafka Producer ===")
    logger.info(f"Kafka Servers: {kafka_servers}")
    logger.info(f"Topic: {kafka_topic}")
    logger.info(f"Data Path: {data_path}")
    logger.info(f"Data Mode: {data_mode}")
    logger.info(f"Producer Rate: {producer_rate} transactions/sec")
    
    # Initialize data simulator and producer
    simulator = InstacartDataSimulator(data_path, mode=data_mode)
    producer = PurchaseTransactionProducer(kafka_servers, kafka_topic)
    
    # Calculate sleep time between messages
    sleep_time = 1.0 / producer_rate if producer_rate > 0 else 0.01
    
    transaction_count = 0
    start_time = time.time()
    
    try:
        logger.info("Starting to produce transactions...")
        
        while True:
            transaction = simulator.generate_transaction()
            if transaction:
                producer.send_transaction(transaction)
                transaction_count += 1
                
                if transaction_count % 1000 == 0:
                    elapsed_time = time.time() - start_time
                    rate = transaction_count / elapsed_time
                    logger.info(
                        f"Produced {transaction_count} transactions | "
                        f"Rate: {rate:.2f} trans/sec | "
                        f"Elapsed: {elapsed_time:.2f}s"
                    )
            
            time.sleep(sleep_time)
    
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    
    finally:
        producer.flush()
        producer.close()
        
        elapsed_time = time.time() - start_time
        logger.info(
            f"=== Producer Statistics ===\n"
            f"Total Transactions: {transaction_count}\n"
            f"Total Time: {elapsed_time:.2f}s\n"
            f"Average Rate: {transaction_count/elapsed_time:.2f} trans/sec"
        )


if __name__ == "__main__":
    main()
