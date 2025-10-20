"""
Fibonacci Calculator Function

A DigitalOcean Function that calculates Fibonacci numbers.
Publicly accessible via HTTP with query parameter.

Usage:
    GET /fibonacci?n=10
    Returns: {"n": 10, "result": 55, "duration_seconds": 0.001}
"""

import json
import os
import time
import socket
import sys


def fibonacci(n: int) -> int:
    """Recursive Fibonacci - exponentially CPU intensive."""
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


def main(event, context):
    """
    Main function handler for DO Functions.

    Args:
        event (dict): Contains request data including:
            - http: dict with method, headers, path, queryString, etc.
        context (dict): Function execution context

    Returns:
        dict: Response with statusCode, body, and headers
    """
    # Instance identifier
    instance_id = f"{socket.gethostname()}-{os.getpid()}"

    print(f"[{instance_id}] Function invoked at {time.strftime('%H:%M:%S')}", flush=True)

    # Extract HTTP headers
    http_data = event.get('__ow_headers', {})

    # API Key Authentication
    expected_api_key = os.getenv('INTERNAL_API_KEY')
    provided_api_key = http_data.get('x-api-key')

    if not expected_api_key or provided_api_key != expected_api_key:
        print(f"[{instance_id}] Authentication failed", flush=True)
        return {
            'statusCode': 403,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Forbidden',
                'message': 'Authentication failed.'
            })
        }

    print(f"[{instance_id}] Authentication successful", flush=True)

    # Capture caller information
    caller_info = {
        "source_ip": http_data.get('x-forwarded-for', 'unknown'),
        "headers": {
            "x-forwarded-for": http_data.get('x-forwarded-for'),
            "x-real-ip": http_data.get('x-real-ip'),
            "do-connecting-ip": http_data.get('do-connecting-ip'),
            "user-agent": http_data.get('user-agent'),
            "host": http_data.get('host'),
        },
        "all_headers": http_data
    }

    # Query parameters are passed as top-level keys in the event
    n_str = event.get('n')

    if n_str is None:
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Missing required parameter "n"',
                'usage': 'GET /fibonacci?n=10',
                'note': 'n should be between 0 and 40 for reasonable performance'
            })
        }

    # Validate and parse n
    try:
        n = int(n_str)
        if n < 0:
            raise ValueError("n must be non-negative")
        if n > 45:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': 'n too large (max 45)',
                    'reason': 'Values above 45 take too long to compute'
                })
            }
    except ValueError as e:
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': f'Invalid parameter "n": {str(e)}',
                'received': n_str
            })
        }

    # Calculate fibonacci with timing
    print(f"[{instance_id}] Starting fibonacci({n}) calculation...", flush=True)
    start_time = time.time()
    result = fibonacci(n)
    duration = time.time() - start_time
    print(f"[{instance_id}] Completed in {duration:.2f}s. Result: {result}", flush=True)

    # Return success response with caller information
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({
            'n': n,
            'result': result,
            'duration_seconds': round(duration, 4),
            'function': 'fibonacci',
            'note': 'Calculated using recursive algorithm',
            'caller_info': caller_info,
            'instance_id': instance_id  # Include instance ID in response
        })
    }
