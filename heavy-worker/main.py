"""
Heavy Worker - Polling + Autoscaling Test

This worker demonstrates the polling pattern for heavy analyzers:
1. Polls MongoDB every 5 seconds for unclaimed requests
2. Atomically claims work (prevents race conditions)
3. Runs fibonacci calculation (simulates heavy work)
4. CPU spike triggers autoscaling
5. Scales down when no work available
6. Runs 3 tasks in parallel per instance (maxes out single CPU)

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
from multiprocessing import Pool
from functools import partial

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


def process_work(work):
    """
    Process the claimed work (run fibonacci calculation).
    This runs in a separate process.

    Args:
        work: The work item to process

    Returns:
        tuple: (request_id, result, duration)
    """
    request_id = work.get("request_id")
    n = work.get("n", 40)

    worker_pid = os.getpid()
    logger.info(f"[{POD_ID}-worker-{worker_pid}] Processing request {request_id}, fibonacci({n})...")

    # Heavy work - CPU spikes here, triggers autoscaling
    start_time = time.time()
    result = fibonacci(n)
    duration = time.time() - start_time

    logger.info(f"[{POD_ID}-worker-{worker_pid}] Completed in {duration:.2f}s. Result: {result}")

    return (request_id, result, duration)


def mark_completed(db, result_tuple):
    """
    Mark work as completed in database.
    Called from main thread after worker finishes.

    Args:
        db: MongoDB database
        result_tuple: (request_id, result, duration)
    """
    request_id, result, duration = result_tuple

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
    logger.info(f"[{POD_ID}] Marked {request_id} as completed")


def main():
    """
    Main worker loop - polls for work and processes it in parallel.

    Runs up to 3 fibonacci calculations concurrently per instance.
    """
    logger.info(f"[{POD_ID}] Worker started with 3 parallel workers...")

    db = get_mongo_db()

    # Create pool of 3 worker processes
    with Pool(processes=3) as pool:
        active_tasks = []  # List of (AsyncResult, work_dict) tuples

        while True:
            try:
                # Clean up completed tasks
                still_active = []
                for async_result, work in active_tasks:
                    if async_result.ready():
                        # Task completed, mark in database
                        try:
                            result_tuple = async_result.get()
                            mark_completed(db, result_tuple)
                        except Exception as e:
                            logger.error(f"[{POD_ID}] Error completing task: {e}", exc_info=True)
                    else:
                        # Still running
                        still_active.append((async_result, work))

                active_tasks = still_active

                # Try to claim more work if we have capacity
                if len(active_tasks) < 3:
                    work = try_claim_work(db)

                    if work:
                        # Submit work to pool
                        logger.info(f"[{POD_ID}] Claimed work, submitting to pool ({len(active_tasks)+1}/3 slots used)")
                        async_result = pool.apply_async(process_work, (work,))
                        active_tasks.append((async_result, work))
                        # Immediately try to claim more work
                        continue

                # If pool is full or no work available, sleep briefly
                if len(active_tasks) >= 3:
                    logger.debug(f"[{POD_ID}] All 3 workers busy, waiting...")
                    time.sleep(1)  # Short sleep, check for completions soon
                else:
                    # No work available and pool not full
                    logger.info(f"[{POD_ID}] No work available, sleeping 5s...")
                    time.sleep(5)

            except Exception as e:
                logger.error(f"[{POD_ID}] Error in main loop: {e}", exc_info=True)
                time.sleep(5)


if __name__ == "__main__":
    main()
