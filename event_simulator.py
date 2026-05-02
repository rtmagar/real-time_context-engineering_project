import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = "user_activity_stream"
USERS = ["user_101", "user_102", "user_103"]

# Simulated actions that an AI agent might need context on
ACTIONS = [
    {"event_type": "page_view", "description": "User navigated to the 'Advanced Spark Data Engineering' course page."},
    {"event_type": "search", "description": "User searched for 'How to handle Kafka backpressure'."},
    {"event_type": "error", "description": "User encountered a 500 Internal Server Error on the checkout page."},
    {"event_type": "add_to_cart", "description": "User added 'System Design Interview Prep' to their cart."},
    {"event_type": "idle", "description": "User has been idle on the pricing page for 45 seconds."}
]

print(f"Starting Real-Time Event Simulator. Streaming to Kafka topic: {TOPIC_NAME}...")

try:
    while True:
        # Generate a random event
        action = random.choice(ACTIONS)
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": random.choice(USERS),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": action["event_type"],
            "raw_text_description": action["description"]
        }
        
        # Send to Kafka
        producer.send(TOPIC_NAME, value=event)
        print(f"Sent: {event['user_id']} -> {event['event_type']}")
        
        # Wait 1 to 4 seconds before the next event
        time.sleep(random.uniform(1, 4))

except KeyboardInterrupt:
    print("\nSimulation stopped.")
finally:
    producer.close()