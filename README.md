# Healthcare-Insurance-Company-ETL

Below is the workflow of the ETL process to migrate and analyze the data from local to AWS ecosystem.
<ul>
  <li>Load all the raw data AWS s3 buckets.</li>
  <li>Data cleaning and transformation with Pyspark and Python.</li>
  <li>Load the final data to Redshift.</li>
  <li>Validate or Test Use cases in Databricks.</li>
</ul>

![Workflow](https://github.com/sonika2580/Healthcare-Insurance-Company-ETL/assets/131336737/b939f493-8c2d-438b-adad-1cae6ef4cd91)

Now let's get started!!!

1. Load all the raw data from local system/databases like SQL Server to AWS s3 buckets.
   To load the data from local to AWS s3 there are several ways.
   <ul>
     <li>Use AWS s3 UI to load data from local.</li>
     <li>Connecting AWS environment with your pyspark and loading the data by writing scripts that handles multiple file formats.</li>
   </ul>

   We can create different folders in the s3 bucket to manage different types/formats of data. Example can be see in the below picture.
       ![image](https://github.com/sonika2580/Healthcare-Insurance-Company-ETL/assets/131336737/9b6bb8dd-6aaf-4fbc-b490-9e3ba8aad2c1)
   
   We may also create subfolders in those folders as input and output folder to store incoming and outgoing information/data. Example can be seen in the below picture.
       ![image](https://github.com/sonika2580/Healthcare-Insurance-Company-ETL/assets/131336737/2986374d-a7be-4d74-af64-8f76f8454d1a)

2.  Data Cleaning and Transformation
    To clean the data and create various layers of transformation we will be using Python and Pyspark. We will be storing the transformed data to our s3 storage and also publish it to our dataware house which is the Redshift.
    There are several common steps that shall be taken to clean the data as follows:
      <ul>
        <li>Check for any null values</li>
        <li>Check for datatypes</li>
        <li>Check the name of the columns</li>
        <li>Check all the categorical values</li>
        <li>Data Integrity</li>
      </ul>
      

   

