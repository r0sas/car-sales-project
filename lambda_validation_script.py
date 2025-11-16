import boto3
import time
import json

athena = boto3.client("athena")
sns = boto3.client("sns")

ATHENA_DB = "car_sales_db"
OUTPUT = "s3://315122956915-athena-results/validations/"
SNS_TOPIC = "arn:aws:sns:REGION:315122956915:data-alerts"

def run_query(query):
    query = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": OUTPUT}
    )
    query_id = query["QueryExecutionId"]
    while True:
            status = athena.get_query_execution(QueryExecutionId=query_id)
            state = status["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                return query_id

            if state in ["FAILED", "CANCELLED"]:
                # return failure and state
                raise Exception(f"Athena query failed: {state}. QueryExecutionId: {query_id}")

            time.sleep(2)

def wait(query_id):
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            return state
        time.sleep(1)

def get_result(query_id):
    results = athena.get_query_results(QueryExecutionId=query_id)
    rows = results["ResultSet"]["Rows"]
    return rows[1]["Data"][0]["VarCharValue"]

def lambda_handler(event, context):
    # 1️⃣ Run queries
    q1 = run_query("SELECT SUM(model_total_units_sold) FROM car_sales_db.sales_summary;")
    q2 = run_query("SELECT COUNT(*) FROM car_sales_db.silver;")

    q3 = run_query("SELECT SUM(price) FROM car_sales_db.silver;")
    q4 = run_query("SELECT SUM(model_total_revenue) FROM car_sales_db.sales_summary;")


    # 2️⃣ Wait
    for q in [q1, q2, q3, q4]:
        wait(q)

    # 3️⃣ Read results
    gold_count = int(get_result(q1))
    silver_count = int(get_result(q2))
    
    silver_rev = float(get_result(q3))
    gold_rev = float(get_result(q4))

    errors = []

    if gold_count != silver_count:
        errors.append(f"Count mismatch: Gold={gold_count}, Silver={silver_count}")

    if round(gold_rev, 2) != round(silver_rev, 2):
        errors.append(f"Revenue mismatch: Gold={gold_rev}, Silver={silver_rev}")

    # 4️⃣ PASS / FAIL
    if errors:
        sns.publish(
            TopicArn=SNS_TOPIC,
            Message="\n".join(errors),
            Subject="❌ Data Validation FAILED"
        )
        return {"status": "FAIL", "errors": errors}

    return {"status": "PASS"}
