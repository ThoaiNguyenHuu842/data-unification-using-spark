from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as func
import mysql.connector
import json
from bson.json_util import dumps

def run_unify(primary_key):
    conf = SparkConf().setMaster("spark://spark-master:7077").setAppName("Unify") \
        .set("spark.dynamicAllocation.enabled", False).set("spark.driver.maxResultSize", "3g").set(
        "spark.driver.memoryOverhead", "3g").set("spark.executor.memory", "3g")
    # initiate Spark session with input is contact database and output is unified contact database
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.input.uri",
                "mongodb://nht:abc123@contact-mongo-db:27017/nht_contact.contact?connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256") \
        .config("spark.mongodb.output.uri",
                "mongodb://nht:abc123@unify-mongo-db:27017/nht_unified_contact.unified_contact?connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256") \
        .config(conf=conf) \
        .getOrCreate()

    # load contact data to data frame
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load(schema=contact_schema)

    # get columns from contact data frame
    columns = [func.collect_list(x).alias(x) for x in df.columns]

    # group contact data frame by primary key
    raw_unified_df = df.groupby(primary_key).agg(*columns)
    unified_contacts = []

    total = df.count()
    total_match = raw_unified_df.count()
    unique = raw_unified_df.where(func.size(raw_unified_df["_id"]) == 1).count()
    match = total_match - unique
    print("Total contacts: " + total)
    print("Total matched contacts: " + match)
    print("Total unique contacts: " + match)

    # load contact activities to data frame
    contact_activities = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("collection", "contact_activity") \
        .load()

    # load transactions from paymetn MySQL database
    transactions = get_transactions();

    # merge data to create unified contact
    for row in raw_unified_df.rdd.collect():
        unified_contact = {}
        for attr, columns in row.__dict__.items():
            for column in columns:
                if column == "_id":
                    column_values = []
                    for items in row[column]:
                        if items[0] is not None:
                            column_values.append(items[0])
                    # store id of contacts used to create this unified contact
                    unified_contact["contact_ids"] = column_values
                else:
                    # merge data using latest value
                    unified_contact[column] = row[column][-1]

        enrich_contact_activities(contact_activities, unified_contact)
        enrich_contact_payment(transactions, unified_contact)
        unified_contacts.append(unified_contact)

    # store unified contact to unified_contact database
    unified_contacts_df = spark.createDataFrame(unified_contacts, unified_contact_schema)
    unified_contacts_df = unified_contacts_df.repartition(100)
    unified_contacts_df.write.format("mongo").mode("append").save()
    spark.stop()

contact_schema = StructType([
    StructField("_id", DoubleType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("company_name", StringType(), True)
])

unified_contact_schema = StructType([
    StructField("contact_ids", ArrayType(IntegerType(), True), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("activities", ArrayType(
        StructType([
            StructField("activity_name", StringType()),
            StructField("created_date", TimestampType())
        ])
    ), True),
    StructField("transactions", StructType([
        StructField("total_transaction", LongType()),
        StructField("total_amount", DecimalType())
    ]), True)
])

def get_transactions():
    payment_connection = mysql.connector.connect(
        host='payment-mysql-db',
        user='nht',
        password='abc123',
        database='nht_payment'
    )
    payment_cursor = payment_connection.cursor()
    payment_cursor.execute("SELECT contact_id, COUNT(1) AS total_transaction, SUM(t.quantity*p.price) AS amount "
                           "FROM transaction t "
                           "JOIN product p ON t.product_id = p.id "
                           "GROUP BY t.contact_id")
    transactions = payment_cursor.fetchall()
    payment_cursor.close()
    payment_connection.close()
    return transactions


def enrich_contact_activities(contact_activities, unified_contact):
    contact_activities = list(filter(lambda o: o["contact_id"] in unified_contact["contact_ids"], contact_activities))
    for c in contact_activities:
        c_json = json.loads(dumps(c))
        activity = {
            "contact_id": c_json['contact_id'],
            "activity_name": c_json['activity_name'],
            "created_date": c['created_date']
        }
        unified_contact["contact_activities"].append(activity)


def enrich_contact_payment(transactions, unified_contact):
    transactions = list(filter(lambda o: o[0] in unified_contact["contact_ids"], transactions))
    if len(transactions) > 0:
        unified_contact["transactions"] = {
            "total_transaction": transactions[0][1],
            "total_amount": transactions[0][2]
        };

# main app
primary_key = ["email"]
run_unify(primary_key)