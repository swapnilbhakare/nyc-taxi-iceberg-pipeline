#!/usr/bin/env python3
"""
FastAPI Service: Query Iceberg data via REST endpoints
"""

from fastapi import FastAPI, HTTPException
from pyiceberg.catalog.hive import HiveCatalog
from datetime import datetime
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="NYC Taxi Analytics API")


def get_catalog():
    """Get Hive catalog connection with retry logic."""
    for attempt in range(5):
        try:
            return HiveCatalog(
                name="hive",
                uri="thrift://hive-metastore:9083",
                warehouse="/opt/hive/data/warehouse"
            )
        except Exception as e:
            if attempt < 4:
                time.sleep(2)
            else:
                raise


@app.get("/health")
def health_check():
    """Check HMS connection and table existence."""
    try:
        catalog = get_catalog()
        namespaces = list(catalog.list_namespaces())
        table = catalog.load_table("nyc_taxi.yellow_trips")
        return {
            "status": "healthy",
            "hms_connection": "ok",
            "table_exists": True,
            "namespaces": namespaces
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "hms_connection": "failed",
            "table_exists": False,
            "error": str(e)
        }


@app.get("/trips/stats")
def get_trip_stats(date: str):
    """Get aggregated statistics for a specific date."""
    try:
        datetime.strptime(date, "%Y-%m-%d")
        
        catalog = get_catalog()
        table = catalog.load_table("nyc_taxi.yellow_trips")
        df = table.scan(row_filter=f"pickup_date = '{date}'").to_pandas()
        
        if df.empty:
            return {
                "date": date,
                "total_trips": 0,
                "avg_fare": 0,
                "avg_duration": 0
            }
        
        return {
            "date": date,
            "total_trips": len(df),
            "avg_fare": round(df["fare_amount"].mean(), 2),
            "avg_duration": round(df["trip_duration_minutes"].mean(), 2)
        }
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"Error fetching trip stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/trips/hourly")
def get_hourly_trips(date: str):
    """Get trip counts by hour for a specific date."""
    try:
        datetime.strptime(date, "%Y-%m-%d")
        
        catalog = get_catalog()
        table = catalog.load_table("nyc_taxi.yellow_trips")
        df = table.scan(row_filter=f"pickup_date = '{date}'").to_pandas()
        
        if df.empty:
            return {
                "date": date,
                "hourly_counts": [{"hour": h, "trips": 0} for h in range(24)]
            }
        
        hourly_counts = df.groupby("pickup_hour").size().to_dict()
        result = [
            {"hour": hour, "trips": hourly_counts.get(hour, 0)}
            for hour in range(24)
        ]
        
        return {"date": date, "hourly_counts": result}
        
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"Error fetching hourly trips: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)