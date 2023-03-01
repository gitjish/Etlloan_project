from pyspark.sql.functions import *
import pyinputplus as pyip

def show_report(df_sp_cc_cust, zip, yr, mm):
    print("inside the function", zip, yr, mm)
    data = df_sp_cc_cust.select('TRANSACTION_ID','transaction_type', \
                                'transaction_value', 'Date').where((df_sp_cc_cust['CUST_ZIP'] == zip) \
                                & (substring(df_sp_cc_cust['TIMEID'],1,4) == str(yr)) \
                                & (substring(df_sp_cc_cust['TIMEID'],5,2) == str(mm).rjust(2,'0')))
    data.show()

#Display result for 2.1(2)
def show_report2(df_sp_cc,ttype):
    print("Transaction details of ", ttype.capitalize())
    data2 = df_sp_cc.select('TRANSACTION_TYPE','transaction_value').where( df_sp_cc['TRANSACTION_TYPE']== ttype).groupby('TRANSACTION_TYPE').agg(count('transaction_type').alias("COUNT"), \
    round(sum('transaction_value'),2).alias("TRANSACTION_VALUE")).show()

#Display result for 2.1(2)2.1(3) 
def show_report3(df_sp_cc_br, state):
    print("Transaction details of state", state)
    data3 = df_sp_cc_br.select('Branch_code','transaction_value').where( df_sp_cc_br['BRANCH_STATE']== state).groupby('branch_code')\
    .agg(count('branch_code').alias("COUNT"),round(sum('transaction_value'),2).alias("TRANSACTION_VALUE")).show()

# def trans_by_branch(state):
#     data1 = sp.sql("SELECT bc1, COUNT(bc1), \
#                 round(SUM(transaction_value),2) \
#                 FROM data_br_view \
#                 WHERE BRANCH_STATE == '{}' \
#                 GROUP BY bc1 \
#                 ORDER BY bc1".format(state))
#     data1.show()

# state = str(input("Enter State : "))
#trans_by_branch(state)   

def validate_ttype(ttype, list_ttype):
    if ttype.capitalize() in list_ttype:
        return True
    else:
        if ttype != '0':
            print(f"Not a valid Transaction type.Choose from {list_ttype} or enter 0 to exit")            
        return False
def validate_state(state, list_state):
    if state.upper() in list_state:
        return True
    else:
        if state != '0':
            print(f"Not a valid State.Choose from {list_state} or enter 0 to exit")            
        return False    
# 2.1 (1) Display the transactions made by customers living in a given zip code for a given month and year. 
# Order by day in descending order.

def test_call(df_sp_cc_cust):
    while True:
        zip = pyip.inputInt("Enter 5-digit zipcode (0 to Exit) : ")
        if zip == 0:
            break
        if len(str(zip)) != 5:
            print("Enter a valid 5-digit zipcode (0 to Exit) : ")
            continue

        yr = 2023
        while ~(yr < 1900 or yr >= 2023):
            yr = pyip.inputInt("Enter year: ")
            # print(year(current_date))
            if yr == 0 or yr in range(1900,2023):
                break
            else:
                print("Enter a valid year between 1900 and 2023. 0 to Exit!")
                continue
        if yr == 0:
            break

        mm = pyip.inputInt("Enter month: ")
        if mm < 1 or mm > 12:
            continue
        show_report(df_sp_cc_cust, zip, yr, mm)
    
#2.1(2)Display the number and total values of transaction for a given type  
def test_call2(df_sp_cc,list_ttype):
    while True:
        ttype = pyip.inputStr("Enter Transaction type (0 to Exit) : ")
        if validate_ttype(ttype, list_ttype):
            print("--------------------------\n Transaction Type - {}".format(ttype))
            show_report2(df_sp_cc,ttype)
        else:    
            if ttype == '0':
               break
        
            continue
#2.1(3)Display the number and total values of transaction for a given type  
def test_call3(df_sp_cc_br,list_state):
    while True:
        state = pyip.inputStr("Enter State (0 to Exit) : ")
        if validate_state(state, list_state):
            print("--------------------------\n State - {}".format(state))
            show_report3(df_sp_cc_br,state)
        else:    
            if state == '0':
               break
        
            continue
                
        
    # while True:
    #     var_ssn = pyip.inputInt("Enter 9-digit SSN (0 to Exit) : ")
        
    #     if validate_ssn(var_ssn, list_ssn):
    #         var_start_dt = pyip.inputDate("Enter Start Date in (YYYY/MM/DD) format : ")
    #         var_end_dt = pyip.inputDate("Enter End Date in (YYYYMMDD) format : ")
    #         print("--------------------------\n SSN - *****{}\n Start Date - {}\n End Date - {}".format(str(var_ssn)[5:], var_start_dt, var_end_dt))
    #         show_info(df_sp_cc_cust, var_ssn, var_start_dt, var_end_dt)
    #     else:
    #         if var_ssn == 0:
    #             break
    #         continue