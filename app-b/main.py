"""App B: Diagnostic Receiver with CPU Load Testing

Simple FastAPI app that returns all request information received.
Now with Fibonacci calculation for CPU load testing and autoscaling.
Fire-and-forget mode: returns 202 immediately, logs results when done.
"""

import asyncio
import random
import socket
import time
import logging
from fastapi import FastAPI, Request, Query, BackgroundTasks
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="VPC Test App B - Diagnostic Receiver")


def fibonacci(n: int) -> int:
    """Recursive Fibonacci - exponentially CPU intensive."""
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


def calculate_and_log_fibonacci(n: int, pod_name: str):
    """Background task: Calculate fibonacci and log result."""
    start_time = time.time()
    result = fibonacci(n)
    duration = time.time() - start_time

    log_data = {
        "app_b_pod_name": pod_name,
        "load_test": {
            "type": "fibonacci",
            "input": n,
            "result": result,
            "duration_seconds": round(duration, 2)
        }
    }

    logger.info(f"FIBONACCI_RESULT: {log_data}")


@app.get("/")
async def root():
    """Simple hello endpoint."""
    return {"app": "test-header-b", "message": "VPC diagnostic receiver"}


@app.get("/diagnostic", status_code=202)
async def diagnostic(
    request: Request,
    background_tasks: BackgroundTasks,
    fib: Optional[int] = Query(None, description="Fibonacci number to calculate (CPU load)")
) -> Dict[str, Any]:
    """Return all request information received.

    Fire-and-forget mode:
    - If fib parameter provided, returns 202 immediately
    - Calculates fibonacci in background
    - Logs result to DO logs when complete

    Query params:
    - fib: If provided, calculate fibonacci(fib) in background - VERY CPU intensive!
           fib(35) ~= 1-2s, fib(38) ~= 5-8s, fib(40) ~= 30s, fib(42) ~= 2min
    """
    # Get this pod's hostname
    app_b_pod_name = socket.gethostname()

    # Get client IP
    client_ip = request.client.host if request.client else "unknown"

    # Fire-and-forget fibonacci calculation
    if fib is not None:
        # Add background task - returns immediately
        background_tasks.add_task(calculate_and_log_fibonacci, fib, app_b_pod_name)

        load_info = {
            "type": "fibonacci_async",
            "input": fib,
            "status": "accepted",
            "message": "Calculation running in background, will log when complete"
        }
    else:
        # Legacy: random sleep delay (synchronous for backward compatibility)
        app_b_delay = random.randint(1, 10)
        await asyncio.sleep(app_b_delay)
        load_info = {
            "type": "sleep",
            "input": app_b_delay,
            "status": "completed",
            "duration_seconds": app_b_delay
        }

    # Extract all headers
    all_headers = dict(request.headers)

    # Extract specific headers of interest
    specific_headers = {
        "x-forwarded-for": request.headers.get("x-forwarded-for"),
        "x-real-ip": request.headers.get("x-real-ip"),
        "do-connecting-ip": request.headers.get("do-connecting-ip"),
        "user-agent": request.headers.get("user-agent"),
        "host": request.headers.get("host"),
    }

    return {
        "app": "test-header-b",
        "app_b_pod_name": app_b_pod_name,
        "load_test": load_info,
        "client_ip": client_ip,
        "specific_headers": specific_headers,
        "all_headers": all_headers,
        "method": request.method,
        "path": str(request.url.path),
        "full_url": str(request.url),
    }


@app.get("/health")
async def health():
    """Health check endpoint for Digital Ocean."""
    return {"status": "healthy"}
