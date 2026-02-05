import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SILVER_PATH", "GOLD_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

silver_path = args["SILVER_PATH"]
gold_path = args["GOLD_PATH"]

df = spark.read.parquet(silver_path)

# ---------------------------
# DIM COUNTRY
# ---------------------------
dim_country = (
    df.select(F.col("Country").alias("country"))
      .dropDuplicates()
)

dim_country.write.mode("overwrite").parquet(gold_path + "dim_country/")

# ---------------------------
# DIM PRODUCT
# ---------------------------
window_prod = Window.partitionBy("StockCode").orderBy(F.desc("InvoiceTS"))

dim_product = (
    df.filter(F.col("StockCode").isNotNull())
      .withColumn("rn", F.row_number().over(window_prod))
      .filter(F.col("rn") == 1)
      .select(
          F.col("StockCode").alias("stock_code"),
          F.col("Description").alias("product_description")
      )
)

dim_product.write.mode("overwrite").parquet(gold_path + "dim_product/")

# ---------------------------
# DIM CUSTOMER
# ---------------------------
window_country = Window.partitionBy("CustomerID").orderBy(F.desc("cnt"))

customer_country = (
    df.groupBy("CustomerID", "Country")
      .agg(F.count("*").alias("cnt"))
      .withColumn("rn", F.row_number().over(window_country))
      .filter(F.col("rn") == 1)
      .select(
          F.col("CustomerID").alias("customer_id"),
          F.col("Country").alias("primary_country")
      )
)

dim_customer = (
    df.groupBy("CustomerID")
      .agg(
          F.min("InvoiceTS").alias("first_purchase_ts"),
          F.max("InvoiceTS").alias("last_purchase_ts"),
          F.countDistinct("InvoiceNo").alias("total_orders")
      )
      .withColumnRenamed("CustomerID", "customer_id")
      .join(customer_country, "customer_id", "left")
)

dim_customer.write.mode("overwrite").parquet(gold_path + "dim_customer/")

# ---------------------------
# DIM DATE
# ---------------------------
dim_date = (
    df.select("invoice_date")
      .dropDuplicates()
      .withColumn("year", F.year("invoice_date"))
      .withColumn("month", F.month("invoice_date"))
      .withColumn("day", F.dayofmonth("invoice_date"))
)

dim_date.write.mode("overwrite").parquet(gold_path + "dim_date/")

# ---------------------------
# FACT SALES
# ---------------------------
fact_sales = (
    df.select(
        F.col("InvoiceNo").alias("invoice_no"),
        "invoice_date",
        F.col("CustomerID").alias("customer_id"),
        F.col("StockCode").alias("stock_code"),
        F.col("Country").alias("country"),
        F.col("Quantity").alias("quantity"),
        F.col("UnitPrice").alias("unit_price"),
        "line_amount",
        "InvoiceTS"
    )
)

(
    fact_sales
    .repartition("invoice_date")
    .write.mode("overwrite")
    .partitionBy("invoice_date")
    .parquet(gold_path + "fact_sales/")
)

job.commit()
