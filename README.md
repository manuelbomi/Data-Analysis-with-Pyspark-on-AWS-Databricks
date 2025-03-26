# Data-Analysis-with-Pyspark-on-AWS-Databricks

Here, we show some pyspark commands that can be used for data analysis on a variety of datasets. Databricks (on AWS community edition platform) is used as the platform for the data anaylsis. It is believed that the analyses methods shown here could be useful for a variety of data analytics instances. 

##### Sample sales data from Kaggle: https://www.kaggle.com/datasets/kyanyoga/sample-sales-data
To set up data on AWS Databricks, 

![1 Load data onto AWS Databricks community edition](https://github.com/user-attachments/assets/2d4622f7-53df-47b8-9100-87148b7050d9)

On AWS Datbricks, click on Catalog and Create Table

![2 Click catalog and create table](https://github.com/user-attachments/assets/8e8c80e3-0db6-421d-9f38-13e62a360bbf)

Creat new table on HDFS or S# and give it an appropriate name
![3 create new table on HDFS or S3 and give it an appropriate name ](https://github.com/user-attachments/assets/2a4963ee-8e8b-42f2-9e34-c4783c8c369f)


![4 click to upload the data from your local repository](https://github.com/user-attachments/assets/c0f9358c-fc5a-4695-b255-dc673385338b)

Click create table in a notebbok and create a cluster
![5 Click create table in notebook and create a cluster](https://github.com/user-attachments/assets/63f1927c-79c4-445b-bcfa-a5a04505ccc4)

Create data with the first ro set to true

First row is header, or spark will allocate some default column names

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
![6 create data with first row is header set to true](https://github.com/user-attachments/assets/fdd08a1a-b80d-498d-bb83-ff2240678e6b)

##### Check data schema
df.printSchema()
##### Create temporary table or view
temp_table_name = "sales_06_FY2020_21_copy_csv"

df.createOrReplaceTempView(temp_table_name)

##### Use SQL SELECT * command to check the dataframe
%sql

select * from `sales_06_FY2020_21_copy_csv`


![7 check the data schema and create a tempview to create a dataframe](https://github.com/user-attachments/assets/49970812-4323-4e52-a2d8-a85391f63a23)


![8 check data with SQL SELECT * command](https://github.com/user-attachments/assets/5e3eb773-7381-484d-8f4c-4ad8c8806636)

##### Create a permanent table name so that data can persist on Databricks HDFS
permanent_table_name = "sales_06_FY2020_21_copy_csv_v2"

###### Persist the data
df.write.format("parquet").saveAsTable(permanent_table_name)




#### Compare Pyspark's show() method with Databrick's display method 
#### show() method
df2 = df
![9 create a permanent table so that data will persist on HDFS](https://github.com/user-attachments/assets/eb77e53b-d1fd-4aa7-9dae-c5417db20216)

##### Data can be converted to other formats for modeling. In this instance, the data is converted to #Python list format
df.toPandas().values.tolist()


![10 dataframe can be converted to list format from dataframe](https://github.com/user-attachments/assets/0e05ad22-43e9-4de5-9f39-4ca4d2959dd8)

##### Convert data to dictionary or JSON format
df.toPandas().to_dict('records')

##### Compare Pyspark's show() method with Databrick's display method 
##### show() method
df2 = df
df2.show()

![12 compare show() and display() methods](https://github.com/user-attachments/assets/fbb425df-8b2f-49fb-891e-d33598298373)

![13 show method](https://github.com/user-attachments/assets/c7dde491-78f7-4749-a0a5-279073150359)


##### Display method
display(df2)
![14 display method](https://github.com/user-attachments/assets/3da8e7cc-dd6d-4a10-b5ff-0cd163b82867)

##### show all the data columns
df2.columns

![15 show data columns](https://github.com/user-attachments/assets/75131cf9-48f1-40a2-820d-6d413094b767)
##### show  the data colums with their data types
df2.dtypes

![16 show columns with data types](https://github.com/user-attachments/assets/3b291983-0c76-49f6-bb26-09988c44c28f)

##### drop some dataframe columns
col_to_be_dropped = ['sku' , 'category', 'payment', 'bi_st', 'cust_id' , 'ref_num', 'Name Prefix', 'E mail', 'Customer Since', 'SSN', 'Phone No.' , 'Place Name', 'County', 'City', 'State', 'Zip', 'Region', 'User Name' ]

df3 = df2.drop(*col_to_be_dropped)

df3.display()

![17 drop some data columns](https://github.com/user-attachments/assets/24a061a3-0e68-46bc-9c22-09a1a94cfe77)

##### convert gender column to format 0, 1 instead of 'M' , 'F'. Write the result to a new dataframe
df4 = df3.na.replace(['M','F'],['1','0'])

df4.display()

![18 convert column data to from alphabet to numeric types](https://github.com/user-attachments/assets/2228543b-e2ef-49fa-bb4b-0a3a7f0a1da3)

##### Rename some columns and map it to previous columns
new_col_names = {'First Name':'first_name','Middle Initial':'middle_initial', 'Last Name': 'last_name'}

new_names = [new_col_names.get(col,col) for col in df3.columns]

df4 = df3.toDF(*new_names)

df4.display()

![19 rename some columns and map it to previous cols](https://github.com/user-attachments/assets/24dc7d2d-655c-47b4-a487-6888548d07a0)


##### use 'withColumnRenamed' command to create a new column 'discount(%)'
df5 = df4.withColumnRenamed('Discount_Percent','discount(%)')

df5.display()

![20 use withColumnRenamed command to create discount col](https://github.com/user-attachments/assets/7e5d58a8-20a5-489d-bb34-c589247acaa4)

##### Filter data to show where age < 60
df5[df5.age<60].display()


![21 filter data age   60](https://github.com/user-attachments/assets/195478aa-eafd-490a-83e9-a3ff29b0eb16)

##### Filter data to show where age > 70
df5[df5.age>70].display()

![22 Filter data age   70](https://github.com/user-attachments/assets/8b3c9a27-e31f-4a83-950f-fe4902998087)

##### Show distinct row of names
df5.select("full_name").distinct().display()

##### Use 'dropDuplicates' command to show distinct row of names
df5.dropDuplicates(["full_name"]).display()



![23 Dropduplicate sto show distnct data rows](https://github.com/user-attachments/assets/4b1375f6-c6da-4aef-b740-6a374f23d969)

##### Create index column
from pyspark.sql.functions import row_number

from pyspark.sql.window import Window

df5_index = df5.withColumn("index", row_number().over(Window.orderBy("full_name")))

df5_index.display()



![24 create index column](https://github.com/user-attachments/assets/61b01cd3-7944-4dda-93ff-c733756ca4b4)

##### Filter data between some age range
age_51_to_70 = df5[(df5.age < 70)&(df5.age > 50)].display()

![25 filter data btw some age range](https://github.com/user-attachments/assets/64d6ba6b-59ef-40e0-a72f-be42749523b5)

##### Normalize price column by dividing each price row with the sum of all item prices
from pyspark.sql import functions as F

df5.withColumn('normalized_price',df5.price/df5.groupBy().agg(F.sum("price")).collect()[0][0]).display()

![26 Normalize price column](https://github.com/user-attachments/assets/0863f334-9f55-4524-9b7c-63c4792bc2fb)

##### Create a new condition column based on some criteria
##### When age is > 50 and < 10, the condition column should display 1, 
##### When quantity ordered is greater than 5, condition column should display 2
##### Otherwise, condition column should display 3

df5.withColumn('condition_column',F.when((df5.age>50)&(df5.age<70),1)\

.when(df5.qty_ordered>5,2)\

.otherwise(3)).display()

![27 Create a new condition column based on some criteria](https://github.com/user-attachments/assets/0a8d7d82-1cd0-4e74-b77f-0197c4e1507f)

##### Create a natural logarithm column
df5.withColumn('log_total',F.log(df5.total)).display()
![28 Create a natural logarithm column](https://github.com/user-attachments/assets/2f2aa7d3-f411-4a03-bfe5-5da5e5bda449)


##### Stop your spark session
spark.stop()


