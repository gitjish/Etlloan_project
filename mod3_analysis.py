import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

def show_high_ttype(df_pd_credit):
    result_high_ttype = df_pd_credit[['TRANSACTION_TYPE', 'TRANSACTION_VALUE']].groupby(df_pd_credit['TRANSACTION_TYPE']).sum()
    result_high_ttype.reset_index(inplace=True)
    #print(result_high_ttype.head())
    result_high_ttype.plot(kind='bar', x='TRANSACTION_TYPE', y='TRANSACTION_VALUE',figsize=(8,6))
    plt.title('Transaction by Type')
    plt.xlabel('Transaction Type')
    plt.ylabel('Transaction Count')
    plt.show()
#"Number of Total customers by State
def show_custbystate(df_pd_cust):
    result_custbystate = df_pd_cust[['CUST_STATE']].groupby(df_pd_cust['CUST_STATE']).count()
    result_custbystate.rename(columns = {'CUST_STATE':'COUNT'}, inplace = True)
    result_custbystate.reset_index(inplace=True)
    #result_custbystate.plot(kind='bar', x='CUST_STATE', y='COUNT', figsize=(10,6))
    plt.figure(figsize=[10,6])
    sns.barplot( x='CUST_STATE', y='COUNT', data=result_custbystate)
    plt.title('Transaction Counts by State')
    plt.xlabel('State')
    plt.ylabel('Transaction')
    plt.show()   
#Top 10 customers
def show_top10cust(df_pd_credit) :
    df_pd_credit['CUST_SSN']=df_pd_credit['CUST_SSN'].astype('string')
    result_top10cust = df_pd_credit[['CUST_SSN','TRANSACTION_VALUE']].groupby(df_pd_credit['CUST_SSN']).sum()
    result_top10cust =result_top10cust.sort_values(by=['TRANSACTION_VALUE'],ascending=False)[:10]
    result_top10cust.reset_index(inplace=True)
    plt.figure(figsize=[12,6])
    sns.barplot(x='CUST_SSN', y='TRANSACTION_VALUE', data=result_top10cust)
    #result_top10cust.plot(kind='bar', x='CUST_SSN', y='TRANSACTION_VALUE', figsize=(12,6))
    plt.title('Top 10 Customers')
    plt.xlabel('Customer Id')
    plt.ylabel('Transaction Value')
    plt.show()
    