# ETL,Analysis,Visualisation
## Loan Application dataset and a Credit Card dataset 
   This Project work with the following technologies to manage an ETL process for a Loan Application dataset and a Credit Card dataset: Python,Pandas, MariaDB, Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries(Matplotlib,Seaborn).
   
## Credit Card Dataset Overview

The Credit Card System database is an independent system developed for managing activities such as registering new customers and approving or canceling requests, etc., using the architecture.

## Overview of LOAN application Data API

Banks want to automate the loan eligibility process (in real-time) based on customer details provided while filling out the online application form. These details are Gender, Marital Status, Education, Number of Dependents, Income, Loan Amount, Credit History, and others. To automate this process, they have the task of identifying the customer segments to those who are eligible for loan amounts so that they can specifically target these customers. 

# Work flow of ETL

![workflow](https://user-images.githubusercontent.com/115896761/222286210-195505f4-d4a2-4db3-87e9-44d4b212610e.png)

# Project Goal 

 ## 1.Extract data from different Data sources.

 ### Sources of data:

   https://drive.google.com/drive/folders/1J4a2UndLvVWszHAL2VxJeVXyAHm3xYIp
   
   CDW_SAPP_CUSTOMER.JSON: This file has the existing customer details.
   
   ![customer_data](https://user-images.githubusercontent.com/115896761/222328443-8b57baf9-fdba-49d3-9082-d19c13e0ea9c.png)

   CDW_SAPP_CREDITCARD.JSON: This file contains all credit card transaction information.
   
   ![credit_data](https://user-images.githubusercontent.com/115896761/222328386-80825c66-0d6d-4700-819c-efb06d0ae3bb.png)

   CDW_SAPP_BRANCH.JSON: Each branch’s information and details are recorded in this file.
   
   ![branch_data](https://user-images.githubusercontent.com/115896761/222328488-65bb3d1c-ddda-40ef-ad82-3153bb2f3e30.png)

    API Endpoint
    
    https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json
    
    Web Scraping ,using the requests library in Python. Requests library is used for making HTTP requests to a specific URL and returns the response.
 ## 2.Transform data.
 
  Credit card data and Loan API Data transformation Challenges:

  Several columns were removed,type casted and renamed according to the specifications found in the mapping document.
  
  https://docs.google.com/spreadsheets/d/1t8UxBrUV6dxx0pM1VIIGZpSf4IKbzjdJ/edit#gid=67293124

  Many rows were not formatted for various columns; we had to do some manipulations for those to use them in our MySQL database tables.
  
  Transformed Credit card data schema
  
  ![credit_data_schema](https://user-images.githubusercontent.com/115896761/222330372-98d261f1-eb42-4320-95b4-732f0511d18c.png)
  
  Transformed Customer data schema
  
  ![customer_schema](https://user-images.githubusercontent.com/115896761/222330734-0c583589-f5f7-4f4d-8398-a952249e5871.png)
  
  Transformed Branch data schema
  
  ![branch_schema](https://user-images.githubusercontent.com/115896761/222330800-be185dce-f16f-4b9a-b391-f6f26205e416.png)

## 3.Load Transformed datasets, to Database(MariaDB).

 Created a Database in SQL(MariaDB), named “creditcard_capstone.”
 
 Created a Python and Pyspark Program to load/write the “Credit Card System Data”  and "Loan API Data" into RDBMS(creditcard_capstone).
 
  • CDW_SAPP_BRANCH
  • CDW_SAPP_CREDIT_CARD
  • CDW_SAPP_CUSTOMER 
  • CDW_SAPP_LOAN_APPLICATION
  
## 4.Application Front-End to see/display Data.

 • Display the transactions made by customers living in a given zip code for a given month and year. Order by day in descending order.
 
 ![211](https://user-images.githubusercontent.com/115896761/222340180-bf96266d-bbac-46cb-b0dc-e21044c4baf0.png)

 • Display the number and total values of transactions for a given type.
 
 ![212](https://user-images.githubusercontent.com/115896761/222340248-60895fc9-11cf-4e6d-9c83-2ef45ffe9948.png)

 • Display the total number and total values of transactions for branches in a given state.
 
 ![213_state](https://user-images.githubusercontent.com/115896761/222340339-e23b201a-783c-430d-b1d9-dcaed9309bb6.png)

 • Check the existing account details of a customer.

 • Modify the existing account details of a customer.

 • Generate a monthly bill for a credit card number for a given month and year.
 
 ![223_bill](https://user-images.githubusercontent.com/115896761/222340403-5674f20d-7938-42fc-b9b4-b513117905ef.png)

 • Display the transactions made by a customer between two dates. Order by year, month, and day in descending order.

![transbetween2dates](https://user-images.githubusercontent.com/115896761/222341278-b7bdd240-60f3-4a8e-b5d8-8cfb736da56f.png)

## 5.Data analysis and Visualization

 Users can make changes from the front end, and they can also view data from the front end. Now, the business analyst team wants to analyze and visualize the 
 data according to the below requirements.

 ### Find and plot which transaction type has a high rate of transactions.
 
 ![Trans_by_Type](https://user-images.githubusercontent.com/115896761/222339844-2af06b90-5ad8-4374-8a53-14094b515148.png)

 ### Find and plot which state has a high number of customers.
 ![TRANSACTION_State](https://user-images.githubusercontent.com/115896761/222314399-40bc8ede-bac4-486f-9bf5-7b735b4f7cb4.png)

 ### Find and plot the sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
 
 ![top10_customers](https://user-images.githubusercontent.com/115896761/222331115-c27095a0-768e-4680-8724-5eb4fa1dbc1e.png)
 
 ### Find and plot the percentage of applications approved for self-employed applicants.
 
 ## Find the percentage of rejection for married male applicants.
 ![married_male_rejection](https://user-images.githubusercontent.com/115896761/222331206-50dd9d8f-8ad2-4ab5-8167-9284931a7ba1.png)

 ## Find and plot the top three months with the largest transaction data.
 
 ![self_employment](https://user-images.githubusercontent.com/115896761/222339943-fb98487e-9dc0-47f8-be29-487a54260726.png)

 ## Find and plot which branch processed the highest total dollar value of healthcare transactions.
 
 ![Top_healthcare](https://user-images.githubusercontent.com/115896761/222340045-212b248d-2bfb-4b0d-bd9a-f7739f26e6e1.png)
 
We did  identify the customer segments to those who are eligible for loan amounts so that they can specifically target these customers.

# References:

  • MariaDB Documentation : https://mariadb.org/
  
  • PySpark : https://spark.apache.org/docs/latest/api/python/index.html
  
  • Apache Spark - Spark SQL : https://spark.apache.org/sql/
  
  • Analyzing and Visualization : https://www.analyticsvidhya.com/blog/2021/08/understanding-bar-plots-in-python-beginners-guide-to-data-visualization/
































