# [Rotten Tomatoes] Best Soccer Movies Data Analysis Project

## Project Overview
This project invloves extracting, transforming and loading (ETL) data from 3 CSV files "movies.csv", "critic_reviews.csv", "user_reviews.csv" using Microsoft Azure services. The data is sourced from Kaggle and is used under the following license: CC BY-NC-SA 4.0.

## Architecture and Tools
- **Data Source:** Kaggle Dataset
- **Storage (Data extracted from):** Amazon S3 bucket
- **Storage (Data loaded and stored into):** Azure Data Lake Storage Account
- **ETL Processing:** Azure Data Factory
- **Pipeline Monitoring:** Azure Monitor
- **Notebook Development/PySpark coding**: Azure Databricks
- **Further Analysis:** Azure Synapse Analytics (Serverless SQL Pool)

## Steps Involved
## 1. Data Extraction
The dataset from Kaggle was stored in Amazon S3 bucket. Then the dataset was extracted from S3 bucket into Azure Data Lake Storage Account.

  ### **Step 1: Create an S3 bucket**
  1. Login to Amazon Web Services.
  2. In the search bar type S3 and click on it.
  3. Give your bucket a name and set all the necessary configurations.
  4. Upon successful creation of the bucket, we can either upload an entire folder or particular files as required.

To set up linked services in Azure we need "**_Access Keys_**" and "**_Secret Access Key_** of the S3 bucket.

  ### **Step 2: Create IAM User**
  1.	Navigate to IAM and from the left panel select the users and create a user. 
  2.	Give a user name and select next to go into permissions section. 
  3.	In the permissions section click ‘Attach policies directly’ and from the list of policies select the ‘**_AmazonS3FullAccess_**’
  4.	Now select create user and create the user. 

  ### **Step 3: Get Access Key ID and Secret Access Key**
  1.	Upon successful creation of the user, click on the user and you will be able to see a create access key. 
  2.	Click on that and you will be able to get the access key and the secret access key. 
  3.	You will be able to download the access key and the secret access key in a CSV format; download it.

  ### **Step 4: Configure Bucket Policy**
  1.	Navigate to S3 bucket and click on your bucket. 
  2.	Go to the permissions sections
  3.	Under Bucket policy, click Edit.
  4.	Add a policy in JSON format to allow the IAM user to access the bucket

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::account-id:user/username"
            },
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        }
    ]
}
```

  5.	Replace account-id with your AWS account ID, username with the IAM username, and your-bucket-name with the name of your bucket.
  6.	Click Save changes

## 2. ETL Process
To create an ETL pipeline that extracts the data from S3 bucket we need to set up "**Azure Data Factory Workspace**".

  ### **Step 1: Create Azure Data Lake Storage Account**
  1.	Login to your azure account and navigate to the storage account.
  2.	Create a storage account and don’t forget to enable the enable hierarchy to create data lake storage account.

  ### **Step 2: Create Azure Data Factory Workspace**
  1.	Navigate to Azure Data Factories and create a Data Factory workspace.
  2.	Upon successful deployment of the workspace, click launch studio.

  ### **Step 3: Create Linked Services in Azure Data Factory**
  1.	Go to the manage section and click on the linked services add Amazon S3 bucket and add the necessary configurations like the access key and secret access key and test the connection.
  2.	Now also add a linked service for your azure data lake storage you created earlier.

![image](https://github.com/user-attachments/assets/e69b17b9-bf07-4893-b336-82d7dde747a0)

  ### **Step 4: Create an ETL pipeline to load data from Amazon S3 and into Azure Data Lake Storage Account**
  1.	Navigate to the ‘Author’ section and click create pipeline and from the move and transform section add copy data. Select the source as amazon s3 and sink as azure data lake storage. We have 3 files so we will be creating 3 copy data activity.

![image](https://github.com/user-attachments/assets/3edfca83-6f8e-4380-af04-52335364be36)

  2.	After creating the ETL pipelines click debug to trigger the pipelines. 
  3.	After creating the pipelines and triggering them upon successful completion the files will be copied from S3 bucket into Azure Data Lake Storage

![image](https://github.com/user-attachments/assets/3ad97ab6-92d4-48cb-b35c-e0710ea3e46c)

## 3. Transforming and Filtering the data using PySpark in Azure Databricks Workspace
  ### movies.csv File:
  1. The **_movieRank_** column is not ranked based on anything so we are not selecting that column.
  2. The movies data has some _null_ values in the **_critic_score_** and **_audience_score_** columns, so filtering it out using the filter function and isNotNull() method.
  3. Since **_critic_score_** and **_audience_score_** columns are of type "string", if we try to order them the code doesn't provide the expected result.
  4. So, first we need to remove the "**%**" symbol and cast the column as "integer"
  5. To ensure correct sorting, we need to convert the percentage strings to numeric values before sorting. Here's how we can do it:

```
.withColumn("critic_score_numeric", regexp_replace(col("critic_score"), "%", "").cast("int"))\
.withColumn("audience_score_numeric", regexp_replace(col("audience_score"), "%", "").cast("int"))\
```

  6. In the dataset "**movies.csv** the movies were not rankes based on any particular column. So, we remove that column and rank the movies based on "**_critic_score_** column.
  7. After the above transformations are performed, we write the output to Azure Data Lake Storage Account.




