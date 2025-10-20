"""
Heavy Worker - Polling + Autoscaling Test

This worker demonstrates the polling pattern for heavy analyzers:
1. Polls MongoDB every 5 seconds for unclaimed requests
2. Atomically claims work (prevents race conditions)
3. Runs fibonacci calculation (simulates heavy work)
4. CPU spike triggers autoscaling
5. Scales down when no work available

Data Model:
    {
        "request_id": "uuid",
        "claimed": false,
        "claimed_by": null,
        "claimed_at": null,
        "completed": false,
        "result": null,
        "n": 40  // fibonacci input
    }
"""

import os
import socket
import time
import logging
from datetime import datetime
from pymongo import MongoClient, ReturnDocument

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MongoDB connection
MONGODB_URI = os.getenv("MONGODB_CONNECTION_STRING")
MONGODB_DB = os.getenv("MONGODB_DATABASE", "heavy_worker_test")

# Pod identifier
POD_ID = f"{socket.gethostname()}-{os.getpid()}"


def fibonacci(n: int) -> int:
    """Recursive Fibonacci - CPU intensive."""
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


def get_mongo_db():
    """Get MongoDB database connection."""
    client = MongoClient(MONGODB_URI)
    return client[MONGODB_DB]


def try_claim_work(db):
    """
    Atomically claim one piece of work from the database.

    Returns:
        dict or None: The claimed work item, or None if no work available
    """
    collection = db.requests

    # Atomic find and update - only one pod can claim each request
    work = collection.find_one_and_update(
        {
            "claimed": False  # Only unclaimed work
        },
        {
            "$set": {
                "claimed": True,
                "claimed_by": POD_ID,
                "claimed_at": datetime.utcnow()
            }
        },
        return_document=ReturnDocument.AFTER
    )

    return work


def process_work(db, work):
    """
    Process the claimed work (run fibonacci calculation).

    Args:
        db: MongoDB database
        work: The work item to process
    """
    request_id = work.get("request_id")
    n = work.get("n", 40)

    logger.info(f"[{POD_ID}] Processing request {request_id}, fibonacci({n})...")

    # Heavy work - CPU spikes here, triggers autoscaling
    start_time = time.time()
    result = fibonacci(n)
    duration = time.time() - start_time

    logger.info(f"[{POD_ID}] Completed in {duration:.2f}s. Result: {result}")

    # Mark as completed
    db.requests.update_one(
        {"request_id": request_id},
        {
            "$set": {
                "completed": True,
                "result": result,
                "duration_seconds": duration,
                "completed_at": datetime.utcnow()
            }
        }
    )


def main():
    """
    Main worker loop - polls for work and processes it.

    This runs continuously as a worker process.
    """
    logger.info(f"[{POD_ID}] Worker started, polling for work...")

    db = get_mongo_db()

    while True:
        try:
            # Try to claim work
            work = try_claim_work(db)

            if work:
                # Found work, process it
                process_work(db, work)
                # Immediately check for more work (no sleep)

            else:
                # No work available, idle for 5 seconds
                logger.info(f"[{POD_ID}] No work available, sleeping 5s...")
                time.sleep(5)

        except Exception as e:
            logger.error(f"[{POD_ID}] Error in worker loop: {e}", exc_info=True)
            time.sleep(5)  # Sleep on error to avoid tight loop


if __name__ == "__main__":
    main()
