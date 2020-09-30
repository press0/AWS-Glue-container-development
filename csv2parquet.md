```pyspark
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

    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>3</td><td>None</td><td>pyspark</td><td>idle</td><td></td><td></td><td>✔</td></tr></table>



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    SparkSession available as 'spark'.



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +------------+
    |databaseName|
    +------------+
    | csv2parquet|
    |     default|
    |    sampledb|
    +------------+
    
    Count:   801
    root
    |-- cusip: string
    |-- price: choice
    |    |-- double
    |    |-- string
    |-- security_type: string
    |-- trade_date: string
    
    null_fields []


```pyspark

```
