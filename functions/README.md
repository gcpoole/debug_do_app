# Debug Functions

Test DigitalOcean Functions for understanding platform behavior.

## Structure

```
functions/
├── project.yml                          # Function project configuration
└── packages/
    └── fibonacci/
        └── __main__.py                  # Fibonacci calculator function
```

## Function: Fibonacci Calculator

**Purpose**: Calculate Fibonacci numbers to test function performance and behavior.

**Endpoint**: `GET /fib/fibonacci?n=<number>`

**Parameters**:
- `n` (required): Integer between 0 and 45

**Example Request**:
```bash
curl "https://<your-app-url>/fib/fibonacci?n=10"
```

**Example Response**:
```json
{
  "n": 10,
  "result": 55,
  "duration_seconds": 0.0001,
  "function": "fibonacci",
  "note": "Calculated using recursive algorithm"
}
```

**Performance Notes**:
- `n=10`: ~0.001s
- `n=30`: ~0.2s
- `n=35`: ~2s
- `n=40`: ~30s
- `n=45`: ~5min (max allowed)

## Deployment

This function is deployed as part of the `vpc-internal-lb-test` app.

See `../app-spec-debug.yaml` for the full app configuration.
