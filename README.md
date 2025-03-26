# Data-Analysis-with-Pyspark-on-AWS-Databricks

Here, we show some pyspark commands that can be used for data analysis on a variety of datasets. Databricks (on AWS community edition platform) is used as the platform for the data anaylsis. It is believed that the analyses methods shown here could be useful for a variety of data analytics instances. 

##### Sample sales data from Kaggle: https://www.kaggle.com/datasets/kyanyoga/sample-sales-data
First row is header, spark will allocate some default column names
file_location = "/FileStore/tables/spark_modeling/salesData/sales_06_FY2020_21_copy.csv"
file_type = "csv"

##### CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

##### The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.option("header", first_row_is_header) \
.option("sep", delimiter) \
.load(file_location)

display(df)
