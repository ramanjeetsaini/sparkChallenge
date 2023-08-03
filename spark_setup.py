from pyspark.sql import SparkSession
from dotenv import load_dotenv
def create_spark_session():
    """Create a Spark Session with 1 leader node and 4 worker nodes"""
    _ = load_dotenv()
    return (
        SparkSession
        .builder
        .appName("SparkApp")
        .master("local[1]")
        .getOrCreate()
    )
spark = create_spark_session()
print('Session Started')