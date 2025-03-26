# Data-Analysis-with-Pyspark-on-AWS-Databricks

Here, we show some pyspark commands that can be used for data analysis on a variety of datasets. Databricks (on AWS community edition platform) is used as the platform for the data anaylsis. It is believed that the analyses methods shown here could be useful for a variety of data analytics instances. 

##### Sample sales data from Kaggle: https://www.kaggle.com/datasets/kyanyoga/sample-sales-data
To set up data on AWS Databricks, 

![1 Load data onto AWS Databricks community edition](https://github.com/user-attachments/assets/2d4622f7-53df-47b8-9100-87148b7050d9)

On AWS Datbricks, click on Catalog and Create Table

![2 Click catalog and create table](https://github.com/user-attachments/assets/8e8c80e3-0db6-421d-9f38-13e62a360bbf)


![3 create new table on HDFS or S3 and give it an appropriate name ](https://github.com/user-attachments/assets/2a4963ee-8e8b-42f2-9e34-c4783c8c369f)


![4 click to upload the data from your local repository](https://github.com/user-attachments/assets/c0f9358c-fc5a-4695-b255-dc673385338b)



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
