from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue
from sentence_transformers import SentenceTransformer
import time
from qdrant_client.models import Filter, FieldCondition, MatchValue, Range

print("Waking up AI Agent...")
# 1. Load the exact same embedding model used by Spark
embedder = SentenceTransformer('all-MiniLM-L6-v2')

# 2. Connect to the active Vector Database
qdrant = QdrantClient(url="http://localhost:6333")
collection_name = "user_context_memory"

def retrieve_user_context(target_user_id, ai_query):
    print(f"\n🧠 AI Agent is thinking... retrieving recent context for {target_user_id}")
    
    # Embed what the AI is trying to figure out
    query_vector = embedder.encode(ai_query).tolist()

    # Calculate exactly 5 minutes ago in UNIX time
    five_minutes_ago = int(time.time()) - (5 * 60)

    # Perform a HYBRID SEARCH (Semantic Vector Search + Hard Metadata Filtering)
    response = qdrant.query_points(
        collection_name=collection_name,
        query=query_vector,
        query_filter=Filter(
            must=[
                # Rule 1: Must be this specific user
                FieldCondition(
                    key="user_id", 
                    match=MatchValue(value=target_user_id) 
                ),
                # Rule 2: Must have happened in the last 5 minutes
                FieldCondition(
                    key="timestamp_unix", 
                    range=Range(gte=five_minutes_ago) 
                )
            ]
        ),
        limit=3 # Grab the top 3 most relevant recent events
    )
    
    # Extract the actual hits from the new response object
    search_results = response.points

    if not search_results:
        print("🤖 AI: I have no prior context for this user. I will introduce myself as a new assistant.")
        return

    print(f"🤖 AI prompt injection ready! Here is what the user was just doing:\n")
    for hit in search_results:
        payload = hit.payload
        # The AI uses the score to know how relevant the memory is, and the payload to know the facts
        print(f"  -> [Relevance: {hit.score:.2f}] {payload['event_type'].upper()}: '{payload['text']}'")
        
    print("\n🤖 AI Final Thought: I will now generate a response based on these exact actions.")

# Simulate the user opening the chat bot. 
# The AI needs to know what they were doing to provide an amazing experience.
if __name__ == "__main__":
    test_user = "user_101" # Change this to user_102 or user_103 to see different memories!
    # ai_question = "What errors or courses was the user just looking at?"
    ai_question = "Did the user add any items to their shopping cart?"
    
    retrieve_user_context(target_user_id=test_user, ai_query=ai_question)