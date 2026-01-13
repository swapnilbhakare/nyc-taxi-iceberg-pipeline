# NYC Taxi Analytics Service

Data pipeline: Parquet → PySpark ETL → Iceberg Table → FastAPI

## Architecture

```
PostgreSQL → Hive Metastore → PySpark ETL → Iceberg Tables → FastAPI
```

## Quick Start

```bash
docker compose up
```

Wait for ETL to complete, then test:

```bash
curl http://localhost:8000/health
curl "http://localhost:8000/trips/stats?date=2024-01-15"
curl "http://localhost:8000/trips/hourly?date=2024-01-15"
```

## Components

| Component | Purpose | Type |
|-----------|---------|------|
| PostgreSQL | HMS backend database | Long-running service |
| Hive Metastore | Iceberg catalog (port 9083) | Long-running service |
| ETL | PySpark job processing 2.9M records | One-time job (exits after completion) |
| API | FastAPI service (port 8000) | Long-running service |

## Data Flow

1. **Extract**: Download NYC Yellow Taxi data (January 2024, 47.6 MB parquet)
2. **Transform**: Clean data, remove nulls/invalid trips, add computed columns
3. **Load**: Write to Iceberg table `nyc_taxi.yellow_trips` partitioned by `pickup_date`
4. **Serve**: FastAPI queries Iceberg via PyIceberg + HMS

## API Endpoints

### `GET /health`
Check system health and HMS connectivity.

**Response:**
```json
{
  "status": "healthy",
  "hms_connection": "ok",
  "table_exists": true,
  "namespaces": [["nyc_taxi"]]
}
```

### `GET /trips/stats?date=YYYY-MM-DD`
Get aggregated trip statistics for a specific date.

**Example:**
```bash
curl "http://localhost:8000/trips/stats?date=2024-01-15"
```

**Response:**
```json
{
  "date": "2024-01-15",
  "total_trips": 75620,
  "avg_fare": 19.2,
  "avg_duration": 17.5
}
```

### `GET /trips/hourly?date=YYYY-MM-DD`
Get trip counts by hour (0-23) for a specific date.

**Example:**
```bash
curl "http://localhost:8000/trips/hourly?date=2024-01-15"
```

**Response:**
```json
{
  "date": "2024-01-15",
  "hourly_counts": [
    {"hour": 0, "trips": 2188},
    {"hour": 1, "trips": 1057},
    ...
  ]
}
```

## Technology Stack

- **PySpark 3.5.0**: Distributed data processing
- **Apache Iceberg 1.4.2**: Table format with ACID transactions
- **Hive Metastore 3.1.3**: Metadata catalog
- **FastAPI 0.104.1**: REST API framework
- **PyIceberg 0.6.1**: Python Iceberg client
- **PostgreSQL 11**: Metastore backend

## Project Structure

```
.
├── docker-compose.yml       # Service orchestration
├── Dockerfile.base          # Shared base image (Python + Java)
├── Dockerfile.etl           # ETL container
├── Dockerfile.api           # API container
├── etl.py                   # PySpark ETL pipeline
├── main.py                  # FastAPI application
├── requirements.txt         # Python dependencies
└── README.md
```

## Performance

- **Dataset**: 2.9M records, 47.6 MB parquet
- **ETL Time**: ~40s (after initial JAR download)
- **Throughput**: ~72,000 records/sec
- **Optimizations**: AQE, caching, 8 shuffle partitions, 2GB memory

## Clean Up

```bash
docker compose down -v
```# nyc-taxi-iceberg-pipeline
