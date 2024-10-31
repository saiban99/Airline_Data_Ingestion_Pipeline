import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node daily_raw_flight_data_from_s3
daily_raw_flight_data_from_s3_node1730368789769 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="airlines",
        table_name="daily_raw",
        transformation_ctx="daily_raw_flight_data_from_s3_node1730368789769",
    )
)

# Script generated for node dim_airport_code_read
dim_airport_code_read_node1730368920766 = glueContext.create_dynamic_frame.from_catalog(
    database="airlines",
    table_name="dev_airlines_airports_dim",
    redshift_tmp_dir="s3://temp-snp-1",
    transformation_ctx="dim_airport_code_read_node1730368920766",
)

# Script generated for node Join
Join_node1730369093038 = Join.apply(
    frame1=daily_raw_flight_data_from_s3_node1730368789769,
    frame2=dim_airport_code_read_node1730368920766,
    keys1=["originairportid"],
    keys2=["airport_id"],
    transformation_ctx="Join_node1730369093038",
)

# Script generated for node dep_airport_schema_changes
dep_airport_schema_changes_node1730369203618 = ApplyMapping.apply(
    frame=Join_node1730369093038,
    mappings=[
        ("carrier", "string", "carrier", "string"),
        ("destairportid", "long", "destairportid", "long"),
        ("depdelay", "long", "dep_delay", "bigint"),
        ("arrdelay", "long", "arr_delay", "bigint"),
        ("city", "string", "dep_city", "string"),
        ("name", "string", "dep_airport", "string"),
        ("state", "string", "dep_state", "string"),
    ],
    transformation_ctx="dep_airport_schema_changes_node1730369203618",
)

# Script generated for node Join
Join_node1730369377190 = Join.apply(
    frame1=dep_airport_schema_changes_node1730369203618,
    frame2=dim_airport_code_read_node1730368920766,
    keys1=["destairportid"],
    keys2=["airport_id"],
    transformation_ctx="Join_node1730369377190",
)

# Script generated for node Change Schema
ChangeSchema_node1730369441542 = ApplyMapping.apply(
    frame=Join_node1730369377190,
    mappings=[
        ("carrier", "string", "carrier", "string"),
        ("destairportid", "long", "destairportid", "long"),
        ("dep_delay", "bigint", "dep_delay", "long"),
        ("arr_delay", "bigint", "arr_delay", "long"),
        ("dep_city", "string", "dep_city", "string"),
        ("dep_airport", "string", "dep_airport", "string"),
        ("dep_state", "string", "dep_state", "string"),
        ("airport_id", "long", "airport_id", "long"),
        ("city", "string", "arr_city", "string"),
        ("name", "string", "arr_airport", "string"),
        ("state", "string", "arr_state", "string"),
    ],
    transformation_ctx="ChangeSchema_node1730369441542",
)

# Script generated for node redshift_fact_table_write
redshift_fact_table_write_node1730369524005 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=ChangeSchema_node1730369441542,
        database="airlines",
        table_name="dev_airlines_daily_flights_fact",
        redshift_tmp_dir="s3://temp-snp-1",
        additional_options={
            "aws_iam_role": "arn:aws:iam::585768157739:role/redshift_role_new"
        },
        transformation_ctx="redshift_fact_table_write_node1730369524005",
    )
)

job.commit()
