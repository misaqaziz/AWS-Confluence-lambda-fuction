import json
import boto3
import concurrent.futures

s3 = boto3.client('s3')

def get_owner_for_bucket(name: str):
    try:
        # Get tags
        resp = s3.get_bucket_tagging(Bucket=name)
        tags = {t["Key"]: t["Value"] for t in resp.get("TagSet", [])}
        owner = tags.get("Owner", "")
    except s3.exceptions.NoSuchTagSet:
        owner = ""
    except Exception:
        # If access denied or other error, don't fail the whole request
        owner = ""
    return {"Name": name, "Owner": owner}

def lambda_handler(event, context):
    # List all buckets (global)
    resp = s3.list_buckets()
    names = [b["Name"] for b in resp.get("Buckets", [])]

    # Fetch tags concurrently to reduce latency
    items = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        for item in ex.map(get_owner_for_bucket, names):
            items.append(item)

    # You can filter down if you only want your 7 buckets (optional):
    # items = [i for i in items if i["Name"].startswith("org-platforms-demo-")]

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            # CORS for Confluence – update this to your domain if you want to restrict it
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,OPTIONS",
            "Cache-Control": "max-age=60"
        },
        "body": json.dumps({"buckets": items})
    }
