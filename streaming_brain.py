from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from sentence_transformers import SentenceTransformer
import time 
from datetime import datetime

# # Suppress warnings for cleaner output
# warnings.filterwarnings("ignore")

print("Initializing AI Embedding Model (this may take a minute to download the first time)...")
# Download/Load the local HuggingFace embedding model
embedder = SentenceTransformer('all-MiniLM-L6-v2')

print("Connecting to Qdrant Vector DB...")
# Connect to our local Docker Qdrant instance
qdrant = QdrantClient(url="http://localhost:6333")
collection_name = "user_context_memory"

# Create the Vector Database Collection if it doesn't exist
try:
    qdrant.get_collection(collection_name)
    print(f"Collection '{collection_name}' found.")
except:
    print(f"Creating collection '{collection_name}'...")
    qdrant.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(size=384, distance=Distance.COSINE) # 384 is the output size of our model
    )


print("Starting Spark Session (Version: 3.5.0)...")
spark = SparkSession.builder \
    .appName("StreamingContextEngine") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

# Define the schema of the JSON coming from Kafka
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("raw_text_description", StringType(), True)
])

# 1. READ STREAM: Connect to Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_activity_stream") \
    .option("startingOffsets", "latest") \
    .load()

# 2. PARSE: Decode Kafka's binary payload to JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 3. TRANSFORM & LOAD: Process each micro-batch
def process_batch(batch_df, batch_id):
    # Convert Spark DataFrame to Pandas for local ML processing
    pdf = batch_df.toPandas()
    
    if pdf.empty:
        return

    print(f"\n--- Processing Micro-Batch {batch_id} | {len(pdf)} events ---")

    # Generate Embeddings from the text description
    texts = pdf['raw_text_description'].tolist()
    embeddings = embedder.encode(texts).tolist()

    # Prepare data for Qdrant
    points = []
    for i, row in pdf.iterrows():
        # Convert the ISO string timestamp to a Unix Integer for Qdrant math
        unix_time = int(datetime.fromisoformat(row['timestamp']).timestamp())

        # Store everything else as metadata (Payload) so the AI can filter by it later
        payload = {
            "user_id": row['user_id'],
            "timestamp": row['timestamp'],
            "timestamp_unix": unix_time, # NEW FIELD: The numeric time!
            "event_type": row['event_type'],
            "text": row['raw_text_description']
        }
        
        
        # PointStruct requires an ID, the Vector, and the Metadata Payload
        points.append(PointStruct(id=row['event_id'], vector=embeddings[i], payload=payload))

    # Upsert to Vector Database
    qdrant.upsert(
        collection_name=collection_name,
        points=points
    )
    print(f"✅ Upserted {len(points)} memories to Qdrant!")

# Start the stream
print("Listening for events... (Press Ctrl+C to stop)")
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()