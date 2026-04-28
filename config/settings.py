KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ecommerce-events"

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "ecommerce_quality"
POSTGRES_USER = "pipeline_user"
POSTGRES_PASSWORD = "pipeline_pass"

# How many events the producer sends per second
PRODUCER_RATE_PER_SECOND = 5

# Intentional bad data ratio (20% will be malformed - to demo validation)
BAD_DATA_RATIO = 0.20

VALID_EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "wishlist"]
VALID_CURRENCIES = ["USD", "INR", "EUR", "GBP"]

# Metrics and alerting
METRICS_INTERVAL = 50
BAD_RATE_ALERT_THRESHOLD_PCT = 25.0
TOP_FAILURE_REASONS_LIMIT = 3

# Kafka consumer behavior
CONSUMER_GROUP_ID = "quality-validator-group"

# earliest: good for demos/backfills; latest: reduces catching up on old messages
KAFKA_AUTO_OFFSET_RESET = "earliest"

# Retries for transient PostgreSQL issues (network blips, contention, etc.)
DB_MAX_RETRIES = 5
DB_RETRY_BASE_SLEEP_SECONDS = 0.2
DB_RETRY_MAX_SLEEP_SECONDS = 5.0
