from pyspark.sql.functions import *
import pyinputplus as pyip
import pymysql
from cap_secrets import username
from cap_secrets import password

def validate_ssn1(var_ssn,list_ssn):
    if var_ssn in list_ssn:
        return True
    else:
        if var_ssn != 0:
            print("Not a valid SSN. Try again or enter 0 to exit")            
        return False

def show_cust_details(df_sp_cust,var_ssn):
    result = df_sp_cust.where(df_sp_cust['SSN'] == var_ssn)
    print("Customer details of :  *****{}".format(str(var_ssn)[5:]))
    
    result['FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'CUST_EMAIL', 'CUST_PHONE', 'FULL_STREET_ADDRESS', 'CUST_CITY', \
                'CUST_STATE', 'CUST_ZIP', 'CREDIT_CARD_NO'].show()
   
def check_cust_details(df_sp_cust,list_ssn):
    while True:
        var_ssn = pyip.inputInt("Enter 9-digit SSN : ")
        if validate_ssn1(var_ssn,list_ssn):
            show_cust_details(df_sp_cust,var_ssn)
            break
        else:
            if var_ssn == 0:
                break
            continue






def validate_ssn(var_ssn,list_ssn):
    if var_ssn in list_ssn:
        return True
    else:
        if var_ssn != 0:
            print("Not a valid SSN. Try again or enter 0 to exit")            
        return False

# def validate_ans(var_ans):
#     if var_ans == 'Y' :
#         return True
#     else:
#         if var_ans != 'N':
#             print("Enter Y to edit or N to exit")            
#         return False

def update_cust_details(df_sp_cust,list_ssn):
    upd_query = ""
    list_col = ['CUST_EMAIL', 'CUST_PHONE', 'FULL_STREET_ADDRESS', 'CUST_CITY', \
                'CUST_STATE', 'CUST_ZIP', 'Done']
    while True:
        var_ssn = pyip.inputInt("Enter 9-digit SSN : ")
        if validate_ssn(var_ssn,list_ssn):
            result = df_sp_cust.where(df_sp_cust['SSN'] == var_ssn)
            show_cust_details(df_sp_cust,var_ssn)
            break
        else:
            if var_ssn == 0:
                break
            continue

    pd_result = result.toPandas()
    while True:
        # 123457849
        var_option = pyip.inputMenu(list_col, numbered=True)
        
        
       
        if var_option == "CUST_EMAIL": 
            print(f"Existing value : {pd_result.loc[0,'CUST_EMAIL']}")
            
            var_email = pyip.inputEmail("Enter new EMAIL : ")
            pd_result.loc[0,'CUST_EMAIL'] = var_email
            print(pd_result.loc[0,'CUST_EMAIL'])
            
        elif var_option == "CUST_PHONE": 
            print(f"Existing value : {pd_result.loc[0,'CUST_PHONE']}")
            var_phone = pyip.inputInt("Enter new PHONE IN 10 digit xxxxxxxxxx (0 to Exit) : ")
            if var_phone == 0:
                continue
            elif len(str(var_phone)) != 10:
                print("Invalid Phone Number!")
                continue
            pd_result.loc[0,'CUST_PHONE'] = '('+str(var_phone)[0:3] + ')'+ str(var_phone)[3:6] + '-' + str(var_phone)[6:]
            print(pd_result.loc[0,'CUST_PHONE'])
            
        elif var_option == "FULL_STREET_ADDRESS":
            print(f"Existing value : {pd_result.loc[0,'FULL_STREET_ADDRESS']}")
            var_address = pyip.inputAddress("Enter new ADDRESS : ")
            pd_result.loc[0,'FULL_STREET_ADDRESS'] = var_address
            print(pd_result.loc[0,'FULL_STREET_ADDRESS'])
            
        elif var_option == "CUST_CITY": 
            print(f"Existing value : {pd_result.loc[0,'CUST_CITY']}")
            var_city = pyip.inputStr("Enter new CITY : ")
            pd_result.loc[0,'CUST_CITY'] = var_city
            print(pd_result.loc[0,'CUST_CITY'] )
            
        elif var_option == "CUST_STATE": 
            print(f"Existing value : {pd_result.loc[0,'CUST_STATE']}")
            var_state = pyip.inputUSState("Enter new STATE : ")
            pd_result.loc[0,'CUST_STATE'] = var_state
            print(pd_result.loc[0,'CUST_STATE'])
            
        elif var_option == "CUST_ZIP": 
            print(f"Existing value : {pd_result.loc[0,'CUST_ZIP']}")
            var_zip = pyip.inputInt("Enter new ZIPCODE (0 to Exit): ")
            if var_zip == 0:
                continue
            elif len(str(var_zip)) not in range(4,6):
                print("Invalid Zip, please enter 4,5 digit Number!")
                continue
            pd_result.loc[0,'CUST_ZIP'] = var_zip
            print(pd_result.loc[0,'CUST_ZIP'])
            
        elif var_option == 'Done':
            upd_query = "UPDATE cdw_sapp_customer SET \
FIRST_NAME = '{}', MIDDLE_NAME = '{}', LAST_NAME = '{}', CUST_EMAIL = '{}', \
CUST_PHONE = '{}', FULL_STREET_ADDRESS = '{}', CUST_CITY = '{}', \
CUST_STATE = '{}', CUST_ZIP = {} WHERE SSN = {}".format(pd_result.loc[0, 'FIRST_NAME'], \
pd_result.loc[0, 'MIDDLE_NAME'], pd_result.loc[0, 'LAST_NAME'], pd_result.loc[0, 'CUST_EMAIL'], \
pd_result.loc[0, 'CUST_PHONE'], pd_result.loc[0, 'FULL_STREET_ADDRESS'], pd_result.loc[0, 'CUST_CITY'], \
pd_result.loc[0, 'CUST_STATE'], pd_result.loc[0, 'CUST_ZIP'], var_ssn)
            #print(upd_query)
            
            try:
                connection = pymysql.connect(
                host='localhost',
                user= username,
                password= password,
                database='creditcard_capstone')
                print("Database connection successful!")

                cursor = connection.cursor()
                cursor.execute(upd_query)
                print("Record updated successfully!!")
                connection.commit()
                cursor.close()
                connection.close()
            except:
                print("Database Connection error. Update failed!")  
            show_cust_details(df_sp_cust,var_ssn)
            break

# def update_cust_details(df_sp_cust,list_ssn):
#     while True:
#         #var_ans = pyip.inputStr("Would you like to update Customer Information? (Y/N) : ")
#         # var_ans
        # if validate_ans(var_ans):
        #     edit_info(df_sp_cust,list_ssn)
        #     #upd_query = ""
        #     break
        # else:
        #     if var_ans == 'N':
        #         break
        #     continue
