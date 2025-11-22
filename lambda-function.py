import os
import json
import io
from datetime import datetime, timezone

import boto3
import pandas as pd
from decimal import Decimal

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

BUCKET = os.environ.get("REPORTS_BUCKET", "your-bucket-name")
RECO_TABLE = dynamodb.Table(os.environ.get("RECO_TABLE", "ai_coo_pricing_recommendations"))

def decimalize(obj):
    # Convert floats to Decimal for DynamoDB
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: decimalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [decimalize(v) for v in obj]
    return obj

def read_csv_from_s3(key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

def compute_recommendations(inventory: pd.DataFrame,
                            sales: pd.DataFrame,
                            cost_price_factor: float,
                            run_id: str):
    # Normalize columns
    inventory.columns = inventory.columns.str.strip().str.lower()
    sales.columns = sales.columns.str.strip().str.lower()

    # Merge on sku
    df = inventory.merge(sales, on="sku", how="inner")

    # Clean numeric fields
    df["ordered product sales"] = (
        df["ordered product sales"]
        .astype(str)
        .replace("[\$,]", "", regex=True)
        .astype(float)
    )
    df["units ordered"] = pd.to_numeric(df["units ordered"], errors="coerce").fillna(0)

    # Avoid division by zero
    df["safe_units"] = df["units ordered"].replace(0, 1)

    # Metrics
    df["price_per_unit"] = df["ordered product sales"] / df["safe_units"]
    df["cost_est"] = df["price_per_unit"] * cost_price_factor
    df["gross_profit_unit"] = df["price_per_unit"] - df["cost_est"]

    recommendations = []

    for _, r in df.iterrows():
        sku = r["sku"]
        available = int(r.get("available", 0))
        units = int(r["units ordered"])
        price = float(r["price_per_unit"])
        gppu = float(r["gross_profit_unit"])

        strategy = "hold"
        price_action = "none"
        price_change_pct = 0.0
        ad_action = "none"
        reason = "Default hold – no strong inventory or margin signal"

        # Rule 1: high inventory, very low sales, good margin → clear inventory
        if available >= 80 and units <= 5 and gppu > 3:
            strategy = "clear_inventory"
            price_action = "drop"
            price_change_pct = -0.10
            ad_action = "boost_low"
            reason = "High inventory, low recent sales, healthy unit margin"

        # Rule 2: medium inventory, low sales → gently stimulate
        elif 30 <= available < 80 and units <= 5 and gppu > 2:
            strategy = "stimulate_demand"
            price_action = "drop"
            price_change_pct = -0.05
            ad_action = "none"
            reason = "Moderate inventory, low sales; small price drop to test elasticity"

        # Rule 3: low inventory & strong margin → premium position
        elif available < 20 and gppu > 5 and units > 0:
            strategy = "premium_position"
            price_action = "increase"
            price_change_pct = 0.05
            ad_action = "none"
            reason = "Low inventory and strong margin; small price increase justified"

        # Rule 4: good velocity & healthy stock → hero SKU, ads ok
        elif 40 <= available <= 100 and units >= 10 and gppu > 2:
            strategy = "hold"
            price_action = "none"
            price_change_pct = 0.0
            ad_action = "boost_low"
            reason = "Balanced inventory and demand; consider slight ad boost to scale hero SKU"

        rec = {
            "run_id": run_id,
            "sku": sku,
            "product_name": str(r.get("product-name", ""))[:400],
            "available": available,
            "units_ordered": units,
            "current_price": round(price, 2),
            "gross_profit_unit": round(gppu, 2),
            "strategy": strategy,
            "price_action": price_action,
            "price_change_pct": round(price_change_pct, 3),
            "ad_action": ad_action,
            "reason": reason,
            "created_at": datetime.now(timezone.utc).isoformat()
        }

        RECO_TABLE.put_item(Item=decimalize(rec))
        recommendations.append(rec)

    return recommendations

def lambda_handler(event, context):
    try:
        if event.get("httpMethod") in ("POST", None):
            body = event.get("body")
            if isinstance(body, str):
                body = json.loads(body or "{}")
        else:
            body = event  # allow direct invoke

        inv_key = body["inventory_s3_key"]
        sales_key = body["sales_s3_key"]
        cost_factor = float(body.get("cost_price_factor", 0.33))

        run_id = datetime.now(timezone.utc).isoformat()

        inventory_df = read_csv_from_s3(inv_key)
        sales_df = read_csv_from_s3(sales_key)

        recs = compute_recommendations(inventory_df, sales_df, cost_factor, run_id)

        resp = {
            "run_id": run_id,
            "sku_count": len(recs),
            "recommendations": recs
        }

        # If called from API Gateway
        if "httpMethod" in event:
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(resp)
            }
        else:
            return resp

    except Exception as e:
        print("Error:", e)
        if "httpMethod" in event:
            return {
                "statusCode": 500,
                "body": json.dumps({"error": str(e)})
            }
        raise
