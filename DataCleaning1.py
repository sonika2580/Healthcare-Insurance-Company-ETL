# configure AWS s3 to databricks


# s3 paths for all files

# csv input path
csv_input_path = "s3://snk-health-data/csv/input/"

# json input path
json_input_path = "s3://snk-health-data/json/input/"

# CSV files
csv_disease = "disease.csv"
csv_group = "group.csv"
csv_hospital = "hospital.csv"
csv_patient_records = "Patient_records.csv"
csv_subgroup = "subgroup.csv"
csv_subscriber = "subscriber.csv"

# JSON files
json_claim = "claims.json"

# csv file path
s3_disease_path = csv_input_path + csv_disease
s3_group_path = csv_input_path + csv_group
s3_hospital_path = csv_input_path + csv_hospital
s3_patient_path = csv_input_path + csv_patient_records
s3_subgroup_path = csv_input_path + csv_subgroup
s3_subscriber_path = csv_input_path + csv_subscriber

# json file path
s3_claims_path = json_input_path + json_claim

# Data Cleaning
# Check for nulls
# Check for duplicates
# Check Datatypes

# --------------------disease.csv------------------------------#
disease_df = spark.read.options(header='True', inferSchema='True').csv(s3_disease_path)
# disease_df.show()
# disease_df.printSchema()

disease_df_new_schema = StructType([
    StructField("SubGrpID", StringType(), nullable=False),
    StructField("Disease_ID", StringType(), nullable=False),
    StructField("Disease_name", StringType(), nullable=True)
])

# Apply the new schema to the dataframe
disease_df_new_schema1 = spark.createDataFrame(disease_df.rdd, disease_df_new_schema)

# disease_df_new_schema1.printSchema()

# No nulls in disease
summary_disease_df = disease_df.describe()
# summary_disease_df.show()

# Write to a Redshift
disease_df_new_schema1.write.format("redshift") \
    .option("url", "jdbc:redshift://redshift-cluster-1.cmbtfkg3qr8o.us-east-1.redshift.amazonaws.com:5439/dev") \
    .option("dbtable", "healthdata.disease") \
    .option("user", "awsuser") \
    .option("password", "Sonika1234") \
    .option("tempdir", "s3a://snk-health-data/csv/tempdir/") \
    .option("aws_iam_role", "arn:aws:iam::926495748791:role/redshiftadmin") \
    .mode("overwrite").save()

# -------------------------------group.csv--------------------------#
group_df = spark.read.options(header='True', inferSchema='True').csv(s3_group_path)
# group_df.show()
# group_df.printSchema()

# check summary of the data
summary_group_df = group_df.describe()
# summary_group_df.show()


# #group_df.printSchema()

group_df_new_schema = StructType([
    StructField("Country", StringType(), nullable=True),
    StructField("premium_written", IntegerType(), nullable=True),
    StructField("zipcode", IntegerType(), nullable=True),
    StructField("Grp_Id", StringType(), nullable=False),  # Change nullable to False
    StructField("Grp_Name", StringType(), nullable=True),
    StructField("Grp_Type", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("year", IntegerType(), nullable=True)
])

# Apply the new schema to the DataFrame
group_df_new_schema1 = spark.createDataFrame(group_df.rdd, group_df_new_schema)
group_df_new_schema1.printSchema()
group_df_new_schema1.show()

# #Write to a Redshift

group_df_new_schema1.write.format("redshift") \
    .option("url", "jdbc:redshift://redshift-cluster-1.cmbtfkg3qr8o.us-east-1.redshift.amazonaws.com:5439/dev") \
    .option("dbtable", "healthdata.groups") \
    .option("user", "awsuser") \
    .option("password", "Sonika1234") \
    .option("tempdir", "s3a://snk-health-data/csv/tempdir/") \
    .option("aws_iam_role", "arn:aws:iam::926495748791:role/redshiftadmin") \
    .mode("overwrite").save()

# ---------------------------------------Hospital.csv-----------------#
hospital_df = spark.read.options(header='True', inferSchema='True').csv(s3_hospital_path)
# hospital_df.show()
# hospital_df.printSchema()

hospital_df.write.format("redshift") \
    .option("url", "jdbc:redshift://redshift-cluster-1.cmbtfkg3qr8o.us-east-1.redshift.amazonaws.com:5439/dev") \
    .option("dbtable", "healthdata.hospital") \
    .option("user", "awsuser") \
    .option("password", "Sonika1234") \
    .option("tempdir", "s3a://snk-health-data/csv/tempdir/") \
    .option("aws_iam_role", "arn:aws:iam::926495748791:role/redshiftadmin") \
    .mode("overwrite").save()

# ------------------------------------Patient Records--------------------#
patient_df = spark.read.options(header='True', inferSchema='True').csv(s3_patient_path)
# patient_df.show()
# patient_df.printSchema()

# summary of patient data
summary_patient_df = patient_df.describe()
# summary_patient_df.show()

# Calculate the count of null values for each column
patient_null_counts = patient_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in patient_df.columns])
# patient_null_counts.show()

# Fill null values with "NA"
patient_filled_df = patient_df.fillna("NA", subset=["Patient_name", "patient_phone"])

patient_filled_df.show()

# Write to redshift
patient_filled_df.write.format("redshift") \
    .option("url", "jdbc:redshift://redshift-cluster-1.cmbtfkg3qr8o.us-east-1.redshift.amazonaws.com:5439/dev") \
    .option("dbtable", "healthdata.patient_records") \
    .option("user", "awsuser") \
    .option("password", "Sonika1234") \
    .option("tempdir", "s3a://snk-health-data/csv/tempdir/") \
    .option("aws_iam_role", "arn:aws:iam::926495748791:role/redshiftadmin") \
    .mode("overwrite").save()

# -----------------subgroup--------------------------------------------#
subgroup_df = spark.read.options(header='True', inferSchema='True').csv(s3_subgroup_path)
subgroup_df.show()
subgroup_df.printSchema()

# Write to redshift
subgroup_df.write.format("redshift") \
    .option("url", "jdbc:redshift://redshift-cluster-1.cmbtfkg3qr8o.us-east-1.redshift.amazonaws.com:5439/dev") \
    .option("dbtable", "healthdata.subgroup") \
    .option("user", "awsuser") \
    .option("password", "Sonika1234") \
    .option("tempdir", "s3a://snk-health-data/csv/tempdir/") \
    .option("aws_iam_role", "arn:aws:iam::926495748791:role/redshiftadmin") \
    .mode("overwrite").save()
# -------------------------------Subscriber------------------------------#
subscriber_df = spark.read.options(header='True', inferSchema='True').csv(s3_subscriber_path)
# subscriber_df.show()
subscriber_df.printSchema()

subscriber_df1 = subscriber_df.withColumnRenamed("sub _id", "sub_id").withColumnRenamed("Zip Code", "ZipCode")

# summary of subscriber data
summary_subscriber_df = subscriber_df1.describe()
# summary_subscriber_df.show()

# Calculate the count of null values for each column
subscriber_null_counts = subscriber_df1.select(
    [sum(col(c).isNull().cast("int")).alias(c) for c in subscriber_df1.columns])
subscriber_null_counts.show()

# Fill null values with "NA"
subscriber_filled_df = subscriber_df1.fillna("NA", subset=["first_name", "Phone", "Subgrp_id", "Elig_ind"])
subscriber_filled_df.show()

# Write to redshift
subscriber_filled_df.write.format("redshift") \
    .option("url", "jdbc:redshift://redshift-cluster-1.cmbtfkg3qr8o.us-east-1.redshift.amazonaws.com:5439/dev") \
    .option("dbtable", "healthdata.subscriber") \
    .option("user", "awsuser") \
    .option("password", "Sonika1234") \
    .option("tempdir", "s3a://snk-health-data/csv/tempdir/") \
    .option("aws_iam_role", "arn:aws:iam::926495748791:role/redshiftadmin") \
    .mode("overwrite").save()

# ---------------------------------------------------Claims----------------------------------------------------------#

claims_df = spark.read.options(header='True', inferSchema='True').json(s3_claims_path)
# claims_df.show()
# claims_df.printSchema()

# summary of claim data
summary_claims_df = claims_df.describe()
summary_claims_df.show()

# Write to redshift
subscriber_filled_df.write.format("redshift") \
    .option("url", "jdbc:redshift://redshift-cluster-1.cmbtfkg3qr8o.us-east-1.redshift.amazonaws.com:5439/dev") \
    .option("dbtable", "healthdata.claims") \
    .option("user", "awsuser") \
    .option("password", "Sonika1234") \
    .option("tempdir", "s3a://snk-health-data/csv/tempdir/") \
    .option("aws_iam_role", "arn:aws:iam::926495748791:role/redshiftadmin") \
    .mode("overwrite").save()
