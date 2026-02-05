import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_path = args["SOURCE_PATH"]
target_path = args["TARGET_PATH"]

df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(source_path)
)

# Standardize column names
for c in df.columns:
    df = df.withColumnRenamed(c, c.strip().replace(" ", "_"))

# Remove cancelled invoices
df = df.withColumn("InvoiceNo", F.col("InvoiceNo").cast("string"))
df = df.filter(~F.col("InvoiceNo").startswith("C"))

# Remove rows without customers
df = df.filter(F.col("CustomerID").isNotNull())

# Enforce correct types
df = df.withColumn("Quantity", F.col("Quantity").cast(IntegerType()))
df = df.withColumn("UnitPrice", F.col("UnitPrice").cast(DoubleType()))
df = df.withColumn("CustomerID", F.col("CustomerID").cast("string"))

# Remove invalid values
df = df.filter((F.col("Quantity") > 0) & (F.col("UnitPrice") > 0))

# Parse invoice timestamp
df = df.withColumn(
    "InvoiceTS",
    F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm")
)

df = df.withColumn("invoice_date", F.to_date("InvoiceTS"))
df = df.withColumn("load_ts", F.current_timestamp())

# Derived metric
df = df.withColumn("line_amount", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))

# Write Silver layer as Parquet partitioned by date
(
    df.select(
        "InvoiceNo","StockCode","Description","Quantity","UnitPrice",
        "CustomerID","Country","InvoiceTS","invoice_date","line_amount","load_ts"
    )
    .repartition("invoice_date")
    .write.mode("overwrite")
    .partitionBy("invoice_date")
    .parquet(target_path)
)

job.commit()
