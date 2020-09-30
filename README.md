# AWS-Glue-container-development

From the September 5 2020 AWS big data blog post 
 - [Developing AWS Glue ETL jobs locally using a container](https://aws.amazon.com/blogs/big-data/developing-aws-glue-etl-jobs-locally-using-a-container)

#### Motivation
AWS Glue ETL development may be accelerated with a local container 

#### Getting Started
Install and start the AWS Glue container with a Jupyter notebook 
```bash
docker run -itd -p 8888:8888 -p 4040:4040 -v ~/.aws:/root/.aws:ro --name glue_jupyter amazon/aws-glue-libs:glue_libs_1.0.0_image_01 /home/jupyter/jupyter_start.sh
```

Display the AWS Glue Data Catalog from the Spark Py REPL  
```bash
docker exec -it  glue_jupyter  bash
/home/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8/bin/pyspark
spark.sql("show databases").show()

+------------+
|databaseName|
+------------+
| csv2parquet|
|     default|
|    sampledb|
+------------+
```

Display the AWS Glue Data Catalog from the Spark Scala REPL
```bash
docker exec -it  glue_jupyter  bash
/home/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8/bin/spark-shell
spark.sql("show databases").show()

+------------+
|databaseName|
+------------+
| csv2parquet|
|     default|
|    sampledb|
+------------+

```

Start Jupyter Notebooks:
```bash
 Jupyter Notebook URL : http://localhost:8888
```

Run an AWS Glue ETL job from a Jupyter Notebook: [csv2parquet.md](csv2parquet.md)

```jupyterpython
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

spark.sql("show databases").show()

glueContext = GlueContext(SparkContext.getOrCreate())
prices_DyF = glueContext.create_dynamic_frame.from_catalog(database="csv2parquet", table_name="csv")
print ("Count:  ", prices_DyF.count())
prices_DyF.printSchema()

job = Job(glueContext)
job.init("csv2parquet")
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "csv2parquet", table_name = "csv", transformation_ctx = "datasource0")
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("cusip", "string", "cusip", "string"), ("price", "double", "price", "double"), ("security_type", "long", "security_type", "long"), ("trade_date", "string", "trade_date", "string")], transformation_ctx = "applymapping1")
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": "s3://press0-test/parquet"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()
```


