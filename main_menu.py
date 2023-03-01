# Main Menu File
#Importing packages
import pyinputplus as pyip
import pyspark as py
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyinputplus as pyip
from cap_secrets import username
from cap_secrets import password
import module21_1
import mod22
import module22_3
import module22_4
import mod3_analysis
import mod5visualization
import pymysql
# Creating Spark Session
sp = SparkSession.builder.appName("ETL_PROJECT").getOrCreate()
# Fetching entire customer data to spark dataframe
df_sp_cust = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CUSTOMER") \
    .option("user", username) \
    .option("password", password) \
    .load()
# Fetching entire credit card data to spark dataframe
df_sp_cc = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
    .option("user", username) \
    .option("password", password) \
    .load()
# Fetching entire branch data to spark dataframe
df_sp_br = sp.read.format("jdbc") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "CDW_SAPP_BRANCH") \
  .option("user", username) \
  .option("password", password) \
  .load()
#2# Fetching joined customer,crdit card data to spark dataframe
query1 = "(SELECT cc.*, cust.cust_zip \
        FROM cdw_sapp_credit_card as cc \
        JOIN cdw_sapp_customer as cust ON cc.CUST_SSN = cust.SSN) as a"
df_sp_cc_cust = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", query1) \
    .option("user", username) \
    .option("password", password) \
    .load()
#2.1(3)Fetching joined branch,crdit card data to spark dataframe
query2= "(SELECT br.branch_code, br.branch_state,br.branch_name,cc.transaction_value \
    FROM cdw_sapp_credit_card as cc \
    JOIN cdw_sapp_branch as br ON cc.BRANCH_CODE = br.BRANCH_CODE) as b"

df_sp_cc_br = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", query2) \
    .option("user", username) \
    .option("password", password) \
    .load()
df_sp_cc_cust = df_sp_cc_cust.withColumn('Date', concat(df_sp_cc_cust['TIMEID'].substr(0,4), lit('-'), \
                                               df_sp_cc_cust['TIMEID'].substr(5,2), lit('-'), \
                                               df_sp_cc_cust['TIMEID'].substr(7,2) \
                                               ))
df_pd_cust = df_sp_cust.toPandas()
df_pd_credit = df_sp_cc.toPandas()
pd_br = df_sp_br.toPandas()
list_state=list(pd_br['BRANCH_STATE'].drop_duplicates())
list_ssn = list(df_pd_cust['SSN'])
list_ttype=list(df_pd_credit['TRANSACTION_TYPE'].drop_duplicates())
list_cc=list(df_pd_credit['CUST_CC_NO'].drop_duplicates())

#-------------------------------------------------------------------
#Creating dataframe for loan data
q1 = "(SELECT self_employed, \
                (round(count(application_status)/(SELECT COUNT(Application_ID) FROM cdw_sapp_loan_application \
                WHERE application_status = 'Y')*100, 2) ) as Percent \
                FROM cdw_sapp_loan_application \
                WHERE Application_status = 'Y' \
                GROUP BY self_employed, application_status) as e"

sp_loan1 = sp.read.format("jdbc") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("user", username) \
  .option("password", password) \
  .option("dbtable", q1) \
  .load()
pd_loan1=sp_loan1.toPandas()

q2 = "(SELECT application_status, COUNT(application_status) AS count_applications, \
		(round(count(application_status)/(SELECT COUNT(*) FROM cdw_sapp_loan_application \
		WHERE Gender = 'Male' AND Married = 'Yes' )*100,2)) AS Percent \
		FROM cdw_sapp_loan_application \
		WHERE Gender = 'Male' AND Married = 'Yes' \
		GROUP BY application_status) as a"

sp_loan2 = sp.read.format("jdbc") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("user", username) \
  .option("password", password) \
  .option("dbtable", q2) \
  .load()

pd_loan2 = sp_loan2.toPandas()

q3 = "(SELECT MONTHNAME(timeid) AS Month, round(sum(TRANSACTION_value),2) AS Transaction_value \
            FROM cdw_sapp_credit_card \
            GROUP BY substr(timeid,1,6) \
            ORDER BY sum(TRANSACTION_value) DESC \
            LIMIT 3) as a"

sp_loan3 = sp.read.format("jdbc") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("user", username) \
  .option("password", password) \
  .option("dbtable", q3) \
  .load()
pd_loan3 = sp_loan3.toPandas()

q4 = "(SELECT branch_code, round(SUM(transaction_value),2) AS Total_Transaction_Value  \
            FROM cdw_sapp_credit_card \
            WHERE transaction_type = 'Healthcare'\
            group by branch_code \
            ORDER BY SUM(transaction_value) DESC \
            LIMIT 5) as a"

sp_loan4 = sp.read.format("jdbc") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("user", username) \
  .option("password", password) \
  .option("dbtable", q4) \
  .load()
pd_loan4 = sp_loan4.toPandas()

list_main_menu = ["Transactions made by customers by Zipcode",
                  "Count and total values of transactions for a given type",
                  "Total number and total values of transactions for branches in a given state",
                  "Check the existing account details of a customer",
                  "Modify the existing account details of a customer",
                  "Generate a monthly bill for a credit card number for a given month and year",
                  "Display the transactions made by a customer between two dates",
                  "Credit Analysis",
                  "Loan Data Visualization",
                  "Exit"]
list_analysis_menu = ["Plot Total transactions by Type",
                      "Number of Total customers by State",
                      "Top 10 customers",
                      "Back to main menu"]
list_visualize_menu=["Loan Approval for Self-employed Applicants",
                     "Married Male Applicants rejected/approved",
                     "Top 3 Months Transactions",
                     "Top Healthcare Transactions by Branch code",
                     "Back to main menu"]
#Displaying main menu
while True:
    selection = pyip.inputMenu(list_main_menu, numbered=True)
    print(selection)
    if selection == 'Exit':
        break
    elif selection == 'Transactions made by customers by Zipcode':
        module21_1.test_call(df_sp_cc_cust)
    elif selection == 'Count and total values of transactions for a given type':
        module21_1.test_call2(df_sp_cc,list_ttype)    
    elif selection == 'Display the transactions made by a customer between two dates':
        module22_4.test_call2(df_sp_cc_cust, list_ssn)
    elif selection == 'Check the existing account details of a customer':
        mod22.check_cust_details(df_sp_cust,list_ssn)  
    elif selection == 'Modify the existing account details of a customer':
        mod22.update_cust_details(df_sp_cust,list_ssn)  
    elif selection == 'Generate a monthly bill for a credit card number for a given month and year':
        module22_3.test_call3(df_sp_cc, list_cc)
    elif selection == "Total number and total values of transactions for branches in a given state":
        module21_1.test_call3(df_sp_cc_br, list_state)
    elif selection == "Credit Analysis":
        while True:
            select_analysis = pyip.inputMenu(list_analysis_menu, numbered=True)  
            print(select_analysis)
            if select_analysis == "Back to main menu":
                break  
            elif select_analysis == "Plot Total transactions by Type":
                mod3_analysis.show_high_ttype(df_pd_credit)
            elif select_analysis == "Number of Total customers by State":
                mod3_analysis.show_custbystate(df_pd_cust)    
            elif select_analysis == "Top 10 customers":
                mod3_analysis.show_top10cust(df_pd_credit)    
    elif selection == "Loan Data Visualization":
        while True:
            select_visualization = pyip.inputMenu(list_visualize_menu, numbered=True)  
            print(select_visualization)
            if select_visualization == "Back to main menu":
                break  
            elif select_visualization == "Loan Approval for Self-employed Applicants":
                mod5visualization.show_self_employed(pd_loan1)
            elif select_visualization == "Married Male Applicants rejected/approved":
                mod5visualization.show_male_applicants(pd_loan2)   
            elif select_visualization == "Top 3 Months Transactions":
                mod5visualization.show_top3_months(pd_loan3)   
            elif select_visualization == "Top Healthcare Transactions by Branch code":
                mod5visualization.show_top_hc_transation(pd_loan4)       

    




