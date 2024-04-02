#Importing
import os
import json
import psycopg2
import pandas as pd
import sqlalchemy
from sqlalchemy import engine
import streamlit as st
from PIL import Image
import plotly.express as px
import plotly.graph_objects as go

#Data_Extraction
#Data_Extraction_From_Github_file
Agg_path="C:/Users/RAMAN/Desktop/DS/PROJECT/Phonepe_Project/pulse/data/aggregated/transaction/country/india/state/"
Agg_state_list=os.listdir(Agg_path)
#Agg_state_list
#Extracting_first_DF(Aggregated_Tx):
Agg_tnx={'State':[], 'Year':[],'Quater':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}
for i in Agg_state_list:
    P_Up=Agg_path+i+"/"
    Agg_year=os.listdir(P_Up)
    for j in Agg_year:
        Pa_Up=P_Up+j+"/"
        Agg_yr_Qt=os.listdir(Pa_Up)
        for k in Agg_yr_Qt:
            Pat_Up=Pa_Up+k
            Data=open(Pat_Up,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
                Name=z['name']
                Transaction_Count=z['paymentInstruments'][0]['count']
                Transaction_Amount=z['paymentInstruments'][0]['amount']
                Agg_tnx['State'].append(i)
                Agg_tnx['Year'].append(j)
                Agg_tnx['Quater'].append(int(k.strip('.json')))
                Agg_tnx['Transaction_type'].append(Name)
                Agg_tnx['Transaction_count'].append(Transaction_Count)
                Agg_tnx['Transaction_amount'].append(Transaction_Amount)
#Creating Dataframe
df_Aggregated_Trans=pd.DataFrame(Agg_tnx)
#df_Aggregated_Trans
#Mapping States name to geojson State name
df_Aggregated_Trans['State'] = df_Aggregated_Trans['State'].replace({'andaman-&-nicobar-islands': 'Andaman & Nicobar Island','andhra-pradesh':'Andhra Pradesh',
                                                                     'arunachal-pradesh':'Arunanchal Pradesh','assam':'Assam','bihar':'Bihar', 
                                                                     'chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                                                     'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 
                                                                     'delhi': 'Delhi', 'goa':'Goa',  'gujarat': 'Gujarat','haryana':'Haryana',
                                                                     'himachal-pradesh':'Himachal Pradesh','jammu-&-kashmir':'Jammu & Kashmir', 
                                                                     'jharkhand':'Jharkhand', 'karnataka':'Karnataka', 'kerala':'Kerala', 
                                                                     'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 
                                                                     'madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra', 
                                                                     'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 
                                                                     'nagaland':'Nagaland','odisha':'Odisha', 'puducherry':'Puducherry', 
                                                                     'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim',
                                                                     'tamil-nadu': 'Tamil Nadu', 'telangana':'Telangana','tripura':'Tripura', 
                                                                     'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand', 
                                                                     'west-bengal':'West Bengal'})

#Data_Extraction_From_Github_file
Usr_path="C:/Users/RAMAN/Desktop/DS/PROJECT/Phonepe_Project/pulse/data/aggregated/user/country/india/state/"
Agg_state_list=os.listdir(Usr_path)
#Extracting_Second_DF(Aggregated_User):
Agg_usr={'State':[], 'Year':[],'Quater':[],'Brand':[], 'Counts':[], 'Percentage':[]}
for i in Agg_state_list:
    P_Up=Usr_path+i+"/"
    Agg_year=os.listdir(P_Up)
    for j in Agg_year:
        Pa_Up=P_Up+j+"/"
        Agg_yr_Qt=os.listdir(Pa_Up)
        for k in Agg_yr_Qt:
            Pat_Up=Pa_Up+k
            Data=open(Pat_Up,'r')
            D=json.load(Data)
            try:
                for z in D['data']['usersByDevice']:
                    Brand=z['brand']
                    Count=z['count']
                    Percentage=z['percentage']
                    Agg_usr['State'].append(i)
                    Agg_usr['Year'].append(j)
                    Agg_usr['Quater'].append(int(k.strip('.json')))
                    Agg_usr['Brand'].append(Brand)
                    Agg_usr['Counts'].append(Count)
                    Agg_usr['Percentage'].append(Percentage)
            except:
                pass
#Creating_DataFrame
df_Aggregated_Usr=pd.DataFrame(Agg_usr)
#df_Aggregated_Usr
#Mapping States name to geojson State name
df_Aggregated_Usr['State'] = df_Aggregated_Usr['State'].replace({'andaman-&-nicobar-islands': 'Andaman & Nicobar Island','andhra-pradesh':'Andhra Pradesh',
                                                                     'arunachal-pradesh':'Arunanchal Pradesh','assam':'Assam','bihar':'Bihar', 
                                                                     'chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                                                     'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 
                                                                     'delhi': 'Delhi', 'goa':'Goa',  'gujarat': 'Gujarat','haryana':'Haryana',
                                                                     'himachal-pradesh':'Himachal Pradesh','jammu-&-kashmir':'Jammu & Kashmir', 
                                                                     'jharkhand':'Jharkhand', 'karnataka':'Karnataka', 'kerala':'Kerala', 
                                                                     'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 
                                                                     'madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra', 
                                                                     'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 
                                                                     'nagaland':'Nagaland','odisha':'Odisha', 'puducherry':'Puducherry', 
                                                                     'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim',
                                                                     'tamil-nadu': 'Tamil Nadu', 'telangana':'Telangana','tripura':'Tripura', 
                                                                     'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand', 
                                                                     'west-bengal':'West Bengal'})

#Data_Extraction_From_Github_file
Map_Tnx_path="C:/Users/RAMAN/Desktop/DS/PROJECT/Phonepe_Project/pulse/data/map/transaction/hover/country/india/state/"
Hover_state_list=os.listdir(Map_Tnx_path)
#Extracting_Third_DF(Map_Tnx):
Map_Agg_tnx={'State':[], 'Year':[],'Quater':[],'District':[], 'Total_Count':[], 'Total_amount':[]}
for i in Hover_state_list:
    P_Up=Map_Tnx_path+i+"/"
    Agg_year=os.listdir(P_Up)
    for j in Agg_year:
        Pa_Up=P_Up+j+"/"
        Agg_yr_Qt=os.listdir(Pa_Up)
        for k in Agg_yr_Qt:
            Pat_Up=Pa_Up+k
            Data=open(Pat_Up,'r')
            D=json.load(Data)
            for z in D['data']['hoverDataList']:
                District=z['name']
                Total_Count=z['metric'][0]['count']
                Total_amount=z['metric'][0]['amount']
                Map_Agg_tnx['State'].append(i)
                Map_Agg_tnx['Year'].append(j)
                Map_Agg_tnx['Quater'].append(int(k.strip('.json')))
                Map_Agg_tnx['District'].append(District)
                Map_Agg_tnx['Total_Count'].append(Total_Count)
                Map_Agg_tnx['Total_amount'].append(Total_amount)
#Creating_DataFrame
df_Map_Tran=pd.DataFrame(Map_Agg_tnx)
#df_Map_Tran
#Mapping States name to geojson State name
df_Map_Tran['State'] = df_Map_Tran['State'].replace({'andaman-&-nicobar-islands': 'Andaman & Nicobar Island','andhra-pradesh':'Andhra Pradesh',
                                                                     'arunachal-pradesh':'Arunanchal Pradesh','assam':'Assam','bihar':'Bihar', 
                                                                     'chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                                                     'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 
                                                                     'delhi': 'Delhi', 'goa':'Goa',  'gujarat': 'Gujarat','haryana':'Haryana',
                                                                     'himachal-pradesh':'Himachal Pradesh','jammu-&-kashmir':'Jammu & Kashmir', 
                                                                     'jharkhand':'Jharkhand', 'karnataka':'Karnataka', 'kerala':'Kerala', 
                                                                     'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 
                                                                     'madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra', 
                                                                     'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 
                                                                     'nagaland':'Nagaland','odisha':'Odisha', 'puducherry':'Puducherry', 
                                                                     'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim',
                                                                     'tamil-nadu': 'Tamil Nadu', 'telangana':'Telangana','tripura':'Tripura', 
                                                                     'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand', 
                                                                     'west-bengal':'West Bengal'})

#Data_Extraction_From_Github_file
Map_Usr_path="C:/Users/RAMAN/Desktop/DS/PROJECT/Phonepe_Project/pulse/data/map/user/hover/country/india/state/"
Hover_state_list=os.listdir(Map_Usr_path)
#Extracting_Fourth_DF(Map_User):
Map_Agg_Usr={'State':[], 'Year':[],'Quater':[],'District':[], 'RegisteredUsers':[], 'appOpens':[]}
for i in Hover_state_list:
    P_Up=Map_Usr_path+i+"/"
    Agg_year=os.listdir(P_Up)
    for j in Agg_year:
        Pa_Up=P_Up+j+"/"
        Agg_yr_Qt=os.listdir(Pa_Up)
        for k in Agg_yr_Qt:
            Pat_Up=Pa_Up+k
            Data=open(Pat_Up,'r')
            D=json.load(Data)
            for z in D['data']['hoverData'].items():
                District=z[0]
                RegisteredUsers=z[1]['registeredUsers']
                appOpens=z[1]['appOpens']
                Map_Agg_Usr['State'].append(i)
                Map_Agg_Usr['Year'].append(j)
                Map_Agg_Usr['Quater'].append(int(k.strip('.json')))
                Map_Agg_Usr['District'].append(District)
                Map_Agg_Usr['RegisteredUsers'].append(RegisteredUsers)
                Map_Agg_Usr['appOpens'].append(RegisteredUsers)
                
#Creating_DataFrame
df_Map_Usr=pd.DataFrame(Map_Agg_Usr)
#df_Map_Usr
#Mapping States name to geojson State name
df_Map_Usr['State'] = df_Map_Usr['State'].replace({'andaman-&-nicobar-islands': 'Andaman & Nicobar Island','andhra-pradesh':'Andhra Pradesh',
                                                                     'arunachal-pradesh':'Arunanchal Pradesh','assam':'Assam','bihar':'Bihar', 
                                                                     'chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                                                     'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 
                                                                     'delhi': 'Delhi', 'goa':'Goa',  'gujarat': 'Gujarat','haryana':'Haryana',
                                                                     'himachal-pradesh':'Himachal Pradesh','jammu-&-kashmir':'Jammu & Kashmir', 
                                                                     'jharkhand':'Jharkhand', 'karnataka':'Karnataka', 'kerala':'Kerala', 
                                                                     'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 
                                                                     'madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra', 
                                                                     'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 
                                                                     'nagaland':'Nagaland','odisha':'Odisha', 'puducherry':'Puducherry', 
                                                                     'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim',
                                                                     'tamil-nadu': 'Tamil Nadu', 'telangana':'Telangana','tripura':'Tripura', 
                                                                     'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand', 
                                                                     'west-bengal':'West Bengal'})


#Data_Extraction_From_Github_file
Top_Tnx_path="C:/Users/RAMAN/Desktop/DS/PROJECT/Phonepe_Project/pulse/data/top/transaction/country/india/state/"
Hover_state_list=os.listdir(Top_Tnx_path)
#Extracting_Fifth_DF(Top_Tnx):
Top_Dist_Tnx={'State':[], 'Year':[],'Quater':[],'District':[], 'Transaction_Count_Pin':[], 'Transaction_amount_Pin':[]}
#Top_Pin_Tnx={'State':[], 'Year':[],'Quater':[],'Pincode':[], 'Transaction_Count_Pin':[], 'Transaction_amount_Pin':[]}
for i in Hover_state_list:
    P_Up=Top_Tnx_path+i+"/"
    Agg_year=os.listdir(P_Up)
    for j in Agg_year:
        Pa_Up=P_Up+j+"/"
        Agg_yr_Qt=os.listdir(Pa_Up)
        #print(Agg_yr_Qt)
        for k in Agg_yr_Qt:
            Pat_Up=Pa_Up+k
            Data=open(Pat_Up,'r')
            D=json.load(Data)
            #print(D)
            for z in D['data']['districts']:
                District=z['entityName']
                Transaction_Count_Dis=z['metric']['count']
                Transaction_Amount_Dis=z['metric']['amount']
                Top_Dist_Tnx['State'].append(i)
                Top_Dist_Tnx['Year'].append(j)
                Top_Dist_Tnx['Quater'].append(int(k.strip('.json')))
                Top_Dist_Tnx['District'].append(District)
                Top_Dist_Tnx['Transaction_Count_Pin'].append(Transaction_Count_Dis)
                Top_Dist_Tnx['Transaction_amount_Pin'].append(Transaction_Amount_Dis)
df_Top_Dis_Tran=pd.DataFrame(Top_Dist_Tnx)
#df_Top_Dis_Tran
#Mapping States name to geojson State name
df_Top_Dis_Tran['State'] = df_Top_Dis_Tran['State'].replace({'andaman-&-nicobar-islands': 'Andaman & Nicobar Island','andhra-pradesh':'Andhra Pradesh',
                                                                     'arunachal-pradesh':'Arunanchal Pradesh','assam':'Assam','bihar':'Bihar', 
                                                                     'chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                                                     'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 
                                                                     'delhi': 'Delhi', 'goa':'Goa',  'gujarat': 'Gujarat','haryana':'Haryana',
                                                                     'himachal-pradesh':'Himachal Pradesh','jammu-&-kashmir':'Jammu & Kashmir', 
                                                                     'jharkhand':'Jharkhand', 'karnataka':'Karnataka', 'kerala':'Kerala', 
                                                                     'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 
                                                                     'madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra', 
                                                                     'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 
                                                                     'nagaland':'Nagaland','odisha':'Odisha', 'puducherry':'Puducherry', 
                                                                     'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim',
                                                                     'tamil-nadu': 'Tamil Nadu', 'telangana':'Telangana','tripura':'Tripura', 
                                                                     'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand', 
                                                                     'west-bengal':'West Bengal'})


#Data_Extraction_From_Github_file
Top_Tnx_path="C:/Users/RAMAN/Desktop/DS/PROJECT/Phonepe_Project/pulse/data/top/transaction/country/india/state/"
Hover_state_list=os.listdir(Top_Tnx_path)
#Extracting_Sixth_DF(Top_Tnx):
Top_Pin_Tnx={'State':[], 'Year':[],'Quater':[],'Pincode':[], 'Transaction_Count_Pin':[], 'Transaction_amount_Pin':[]}
#Top_Pin_Tnx={'State':[], 'Year':[],'Quater':[],'Pincode':[], 'Transaction_Count_Pin':[], 'Transaction_amount_Pin':[]}
for i in Hover_state_list:
    P_Up=Top_Tnx_path+i+"/"
    Agg_year=os.listdir(P_Up)
    for j in Agg_year:
        Pa_Up=P_Up+j+"/"
        Agg_yr_Qt=os.listdir(Pa_Up)
        #print(Agg_yr_Qt)
        for k in Agg_yr_Qt:
            Pat_Up=Pa_Up+k
            Data=open(Pat_Up,'r')
            D=json.load(Data)
            #print(D)
            for z in D['data']['pincodes']:
                pincodes=z['entityName']
                Transaction_Count_Pin=z['metric']['count']
                Transaction_Amount_Pin=z['metric']['amount']
                Top_Pin_Tnx['State'].append(i)
                Top_Pin_Tnx['Year'].append(j)
                Top_Pin_Tnx['Quater'].append(int(k.strip('.json')))
                Top_Pin_Tnx['Pincode'].append(pincodes)
                Top_Pin_Tnx['Transaction_Count_Pin'].append(Transaction_Count_Pin)
                Top_Pin_Tnx['Transaction_amount_Pin'].append(Transaction_Amount_Pin)
df_Top_Pin_Tran=pd.DataFrame(Top_Pin_Tnx)
#df_Top_Pin_Tran
#Mapping States name to geojson State name
df_Top_Pin_Tran['State'] = df_Top_Pin_Tran['State'].replace({'andaman-&-nicobar-islands': 'Andaman & Nicobar Island','andhra-pradesh':'Andhra Pradesh',
                                                                     'arunachal-pradesh':'Arunanchal Pradesh','assam':'Assam','bihar':'Bihar', 
                                                                     'chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                                                     'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 
                                                                     'delhi': 'Delhi', 'goa':'Goa',  'gujarat': 'Gujarat','haryana':'Haryana',
                                                                     'himachal-pradesh':'Himachal Pradesh','jammu-&-kashmir':'Jammu & Kashmir', 
                                                                     'jharkhand':'Jharkhand', 'karnataka':'Karnataka', 'kerala':'Kerala', 
                                                                     'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 
                                                                     'madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra', 
                                                                     'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 
                                                                     'nagaland':'Nagaland','odisha':'Odisha', 'puducherry':'Puducherry', 
                                                                     'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim',
                                                                     'tamil-nadu': 'Tamil Nadu', 'telangana':'Telangana','tripura':'Tripura', 
                                                                     'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand', 
                                                                     'west-bengal':'West Bengal'})

#Data_Extraction_From_Github_file
Top_Usr_path="C:/Users/RAMAN/Desktop/DS/PROJECT/Phonepe_Project/pulse/data/top/user/country/india/state/"
Hover_state_list=os.listdir(Top_Usr_path)

#Extracting_Seventh_DF(Top_User):
Top_Agg_Usr={'State':[], 'Year':[],'Quater':[],'Name':[], 'RegisteredUsers':[]}
for i in Hover_state_list:
    P_Up=Top_Usr_path+i+"/"
    Agg_year=os.listdir(P_Up)
    for j in Agg_year:
        Pa_Up=P_Up+j+"/"
        Agg_yr_Qt=os.listdir(Pa_Up)
        for k in Agg_yr_Qt:
            Pat_Up=Pa_Up+k
            Data=open(Pat_Up,'r')
            D=json.load(Data)
            for z in D['data']['pincodes']:
                Name=z['name']
                RegisteredUsers=z['registeredUsers']
                Top_Agg_Usr['State'].append(i)
                Top_Agg_Usr['Year'].append(j)
                Top_Agg_Usr['Quater'].append(int(k.strip('.json')))
                Top_Agg_Usr['Name'].append(Name)
                Top_Agg_Usr['RegisteredUsers'].append(RegisteredUsers)

#Creating Dataframe
df_Top_User=pd.DataFrame(Top_Agg_Usr)
#df_Top_User
#Mapping States name to geojson State name
df_Top_User['State'] = df_Top_User['State'].replace({'andaman-&-nicobar-islands': 'Andaman & Nicobar Island','andhra-pradesh':'Andhra Pradesh',
                                                                     'arunachal-pradesh':'Arunanchal Pradesh','assam':'Assam','bihar':'Bihar', 
                                                                     'chandigarh':'Chandigarh','chhattisgarh':'Chhattisgarh',
                                                                     'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 
                                                                     'delhi': 'Delhi', 'goa':'Goa',  'gujarat': 'Gujarat','haryana':'Haryana',
                                                                     'himachal-pradesh':'Himachal Pradesh','jammu-&-kashmir':'Jammu & Kashmir', 
                                                                     'jharkhand':'Jharkhand', 'karnataka':'Karnataka', 'kerala':'Kerala', 
                                                                     'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 
                                                                     'madhya-pradesh':'Madhya Pradesh','maharashtra':'Maharashtra', 
                                                                     'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 
                                                                     'nagaland':'Nagaland','odisha':'Odisha', 'puducherry':'Puducherry', 
                                                                     'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim',
                                                                     'tamil-nadu': 'Tamil Nadu', 'telangana':'Telangana','tripura':'Tripura', 
                                                                     'uttar-pradesh':'Uttar Pradesh','uttarakhand':'Uttarakhand', 
                                                                     'west-bengal':'West Bengal'})

#Data_Cleaning and Pre Processing
#df_Aggregated_Trans
print(df_Aggregated_Trans.isnull().sum())

#df_Aggregated_Usr
print(df_Aggregated_Usr.isnull().sum())

#df_Map_Tran
print(df_Map_Tran.isnull().sum())

#df_Map_Usr
print(df_Map_Usr.isnull().sum())

#df_Top_Dis_Tran
print(df_Top_Dis_Tran.isnull().sum())

#df_Top_Pin_Tran
print(df_Top_Pin_Tran.isnull().sum())

#Cleaning Top_Pin_Tran (Pincode data)
df_Top_Pin_Tran['Pincode']=df_Top_Pin_Tran['Pincode'].fillna(df_Top_Pin_Tran['Pincode'].mode()[0])
#df_Top_Pin_Tran
print(df_Top_Pin_Tran.isnull().sum())

#df_Top_User
print(df_Top_User.isnull().sum())

#Transfering Data to Database
import mysql.connector
# Establish a connection to your MySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    auth_plugin="mysql_native_password"
)
mycursor = mydb.cursor()
#Creating database
query1='CREATE DATABASE IF NOT EXISTS Phone_Pe_Project'
mycursor.execute(query1)
mydb.commit()

#Inserting using sqlalchemy
#Aggreagted_Transaction
import sqlalchemy
from sqlalchemy import create_engine
engine =sqlalchemy. create_engine('mysql+mysqlconnector://root:root@localhost/phone_pe_project')

df_Aggregated_Trans.to_sql('aggregated_transaction', engine, if_exists = 'replace', index=False,dtype={
                                                                    "State":sqlalchemy.types.VARCHAR(length=50),
                                                                    "Year":sqlalchemy.types.Integer,
                                                                    "Quater":sqlalchemy.types.Integer,
                                                                    "Transaction_type":sqlalchemy.types.VARCHAR(length=50),
                                                                    "Transaction_count":sqlalchemy.types.BIGINT,
                                                                    "Transaction_amount":sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
                                                                    
#Inserting using sqlalchemy
#Aggreagted_User
import sqlalchemy
from sqlalchemy import create_engine
engine =sqlalchemy. create_engine('mysql+mysqlconnector://root:root@localhost/phone_pe_project')

df_Aggregated_Usr.to_sql('aggregated_user', engine, if_exists = 'replace', index=False,dtype={
                                                                    "State":sqlalchemy.types.VARCHAR(length=50),
                                                                    "Year":sqlalchemy.types.Integer,
                                                                    "Quater":sqlalchemy.types.Integer,
                                                                    "Brand":sqlalchemy.types.VARCHAR(length=50),
                                                                    "Counts":sqlalchemy.types.BIGINT,
                                                                    "Percentage":sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
                                                                    
#Inserting using sqlalchemy
#Map_Transaction
import sqlalchemy
from sqlalchemy import create_engine
engine =sqlalchemy. create_engine('mysql+mysqlconnector://root:root@localhost/phone_pe_project')

df_Map_Tran.to_sql('map_transaction', engine, if_exists = 'replace', index=False,dtype={
                                                                    "State":sqlalchemy.types.VARCHAR(length=50),
                                                                    "Year":sqlalchemy.types.Integer,
                                                                    "Quater":sqlalchemy.types.Integer,
                                                                    "District":sqlalchemy.types.VARCHAR(length=50),
                                                                    "Total_Count":sqlalchemy.types.BIGINT,
                                                                    "Total_amount":sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
                                                                    
#Inserting using sqlalchemy
#Map_User
import sqlalchemy
from sqlalchemy import create_engine
engine =sqlalchemy. create_engine('mysql+mysqlconnector://root:root@localhost/phone_pe_project')

df_Map_Usr.to_sql('map_user', engine, if_exists = 'replace', index=False,dtype={
                                                                    "State":sqlalchemy.types.VARCHAR(length=50),
                                                                    "Year":sqlalchemy.types.Integer,
                                                                    "Quater":sqlalchemy.types.Integer,
                                                                    "District":sqlalchemy.types.VARCHAR(length=50),
                                                                    "RegisteredUsers":sqlalchemy.types.BIGINT,
                                                                    "appOpens":sqlalchemy.types.BIGINT})
                                                                    
#Inserting using sqlalchemy
#Top_Districtwise_Transaction
import sqlalchemy
from sqlalchemy import create_engine
engine =sqlalchemy. create_engine('mysql+mysqlconnector://root:root@localhost/phone_pe_project')

df_Top_Dis_Tran.to_sql('top_district_transaction', engine, if_exists = 'replace', index=False,dtype={
                                                                    "State":sqlalchemy.types.VARCHAR(length=50),
                                                                    "Year":sqlalchemy.types.Integer,
                                                                    "Quater":sqlalchemy.types.Integer,
                                                                    "districts":sqlalchemy.types.VARCHAR(length=50),
                                                                    "Transaction_Count_Pin":sqlalchemy.types.BIGINT,
                                                                    "Transaction_amount_Pin":sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
                                                                    
#Inserting using sqlalchemy
#Top_Pincodewise_Transaction
import sqlalchemy
from sqlalchemy import create_engine
engine =sqlalchemy. create_engine('mysql+mysqlconnector://root:root@localhost/phone_pe_project')

df_Top_Pin_Tran.to_sql('top_pincode_transaction', engine, if_exists = 'replace', index=False,dtype={
                                                                    "State":sqlalchemy.types.VARCHAR(length=50),
                                                                    "Year":sqlalchemy.types.Integer,
                                                                    "Quater":sqlalchemy.types.Integer,
                                                                    "Pincode":sqlalchemy.types.BIGINT,
                                                                    "Transaction_Count_Pin":sqlalchemy.types.BIGINT,
                                                                    "Transaction_amount_Pin":sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
                                                                    
#Inserting using sqlalchemy
#Top_User
import sqlalchemy
from sqlalchemy import create_engine
engine =sqlalchemy. create_engine('mysql+mysqlconnector://root:root@localhost/phone_pe_project')

df_Top_User.to_sql('top_user', engine, if_exists = 'replace', index=False,dtype={
                                                                    "State":sqlalchemy.types.VARCHAR(length=50),
                                                                    "Year":sqlalchemy.types.Integer,
                                                                    "Quater":sqlalchemy.types.Integer,
                                                                    "Name":sqlalchemy.types.VARCHAR(length=50),
                                                                    "RegisteredUsers":sqlalchemy.types.BIGINT,})
                                                                    
import streamlit as st

# Setting page configuration
st.set_page_config(layout="wide")
st.image(Image.open("C:/Users/RAMAN/Desktop/DS/PROJECT/Phonepe_Project/Phone_script/PhonePe_Logo.jpg"), width=500)

# Using Markdown to set the color of the title
st.markdown('<h1 style="color:#441273;">PHONEPE DATA VISUALIZATION AND EXPLORATION</h1>', unsafe_allow_html=True)
# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["***HOME***", "***EXPLORE_DATA***", "***TOP_CHARTS***","***ANALYSIS***"])

with tab1:
    col1, col2 = st.columns(2)
    col2.image(Image.open("C:/Users/RAMAN/Desktop/DS/PROJECT/Phonepe_Project/Phone_script/PhonePe-logo.jpg"))
    with col1:
        # Using Markdown to set the color of the title
        st.subheader("PhonePe is a multinational financial services and digital payments company based in Bengaluru, Karnataka, India. Sameer Nigam, Rahul Chari, and Burzin Engineer started PhonePe in December 2015. In August 2016, the PhonePe application went live, utilizing the Unified Payments Interface (UPI).There are eleven Indian languages available for the PhonePe app. Users can send and receive money, top up data cards, recharge DTH and mobile services, pay for utilities, make in-store purchases, and carry out a variety of financial operations with it.")
        st.markdown("[DOWNLOAD PHONEPE APP](https://www.phonepe.com/app-download/)")
with tab2:
    with st.sidebar:
        selected_Year = st.selectbox("Select Year", ["2018", "2019", "2020", "2021", "2022","2023"])
        selected_Quater = st.selectbox("Select Quater", ["1", "2", "3", "4"])
        selected_Type = st.selectbox("Select Type", [ "map_transaction", "map_user"])
    # Specify the correct column name for coloring based on the selected type
    color_column = "Total_amount" if selected_Type == "map_transaction" else "RegisteredUsers"

    sql = f"SELECT * FROM {selected_Type} WHERE year = {selected_Year} AND quater = {selected_Quater}"
    engine =sqlalchemy. create_engine('mysql+mysqlconnector://root:root@localhost/phone_pe_project')
    mysql_df = pd.read_sql_query(sql, engine.connect(), index_col=None, chunksize=None)
    print(mysql_df)
    fig = px.choropleth(
        mysql_df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color=color_column,
        range_color= (0,13000000),
        color_continuous_scale='viridis'
    )

    fig.update_geos(fitbounds="locations",visible=False)  # To show up the Indian boundaries
    st.plotly_chart(fig,use_container_width=True)
with tab3:
    col1, col2 = st.columns(2)

    with col1:
        import streamlit as st
        # Define function to generate list of particular states
        def Particular_state():
            # Your logic to fetch particular states goes here
            particular_states = [ 'Andaman & Nicobar Island','Andhra Pradesh','Arunanchal Pradesh','Assam','Bihar','Chandigarh','Chhattisgarh',
                                  'Dadra and Nagar Haveli and Daman and Diu','Delhi', 'Goa',   'Gujarat','Haryana','Himachal Pradesh','Jammu & Kashmir', 
                                  'Jharkhand','Karnataka','Kerala','Ladakh', 'Lakshadweep','Madhya Pradesh','Maharashtra', 
                                  'Manipur', 'Meghalaya', 'Mizoram','Nagaland','Odisha', 'Puducherry','Punjab', 'Rajasthan', 'Sikkim',
                                  'Tamil Nadu', 'Telangana','Tripura','Uttar Pradesh','Uttarakhand','West Bengal']  # Example list of states
            return particular_states
        # Add options for select box
        options = ['All India'] + Particular_state()
        # Create select box
        select_state = st.selectbox("State", options)
        select_Year = st.selectbox("Year", ["2018", "2019", "2020", "2021", "2022", "2023"])
        select_Quater = st.selectbox("Quater", ["1", "2", "3", "4"])
        select_Type = st.selectbox("Type", ["aggregated_transaction", "aggregated_user", "map_transaction", "map_user", "top_district_transaction", "top_pincode_transaction", "top_user"])
        select_graph = st.selectbox("Graph", ['Bar', "Pie"])

    with col2:
        def Bar_Plot(select_Year, select_Quater, select_Type):
            engine = sqlalchemy.create_engine('mysql+mysqlconnector://root:root@localhost/phone_pe_project')

            if select_Type == "aggregated_transaction":
                query = f"SELECT Transaction_type, COUNT(*) as Count FROM aggregated_transaction where year={select_Year}  AND quater = {select_Quater}  GROUP BY Transaction_type "
                df = pd.read_sql_query(query, engine)
                fig = px.bar(df, x='Count', y="Transaction_type", title="Bar Plot")
                # Set the x-axis range
                fig.update_xaxes(range=[0, 50]) 
                st.plotly_chart(fig)
            elif select_Type == "aggregated_user":
                query = f"SELECT Brand, COUNT(*) as Count FROM aggregated_user where year={select_Year}  AND quater = {select_Quater}  GROUP BY Brand "
                df = pd.read_sql_query(query, engine)
                fig = px.bar(df, x='Count', y="Brand", title="Bar Plot")
                # Set the x-axis range
                fig.update_xaxes(range=[0, 50]) 
                st.plotly_chart(fig)
            elif select_Type == "map_transaction":
                query = f"SELECT District, Total_amount FROM map_transaction where year={select_Year}  AND quater = {select_Quater} "
                df = pd.read_sql_query(query, engine)
                fig = px.bar(df, x='District', y="Total_amount", title="Bar Plot")
                # Set the x-axis range
                #fig.update_xaxes(range=[0, 50]) 
                st.plotly_chart(fig)
            elif select_Type == "map_user":
                query = f"SELECT District, RegisteredUsers FROM map_user where year={select_Year}  AND quater = {select_Quater} "
                df = pd.read_sql_query(query, engine)
                fig = px.bar(df, x='District', y="RegisteredUsers", title="Bar Plot")
                # Set the x-axis range
                #fig.update_xaxes(range=[0, 50]) 
                st.plotly_chart(fig)
            elif select_Type == "top_district_transaction":
                query = f"SELECT District, Transaction_Count_Pin FROM top_district_transaction where year={select_Year}  AND quater = {select_Quater} "
                df = pd.read_sql_query(query, engine)
                fig = px.bar(df, x='District', y="Transaction_Count_Pin", title="Bar Plot")
                # Set the x-axis range
                #fig.update_xaxes(range=[0, 50]) 
                st.plotly_chart(fig)
            elif select_Type == "top_pincode_transaction":
                query = f"SELECT State, Transaction_amount_Pin FROM top_pincode_transaction where year={select_Year}  AND quater = {select_Quater} "
                df = pd.read_sql_query(query, engine)
                fig = px.bar(df, x='State', y="Transaction_amount_Pin", title="Bar Plot")
                # Set the x-axis range
                #fig.update_xaxes(range=[0, 50]) 
                st.plotly_chart(fig)
            elif select_Type == "top_user":
                query = f"SELECT State, RegisteredUsers FROM top_user where year={select_Year}  AND quater = {select_Quater} "
                df = pd.read_sql_query(query, engine)
                fig = px.bar(df, x='State', y="RegisteredUsers", title="Bar Plot")
                # Set the x-axis range
                #fig.update_xaxes(range=[0, 50]) 
                st.plotly_chart(fig)

        def Pie_plot(select_Year, select_Quater, select_Type):
            engine = sqlalchemy.create_engine('mysql+mysqlconnector://root:root@localhost/phone_pe_project')

            if select_Type == "aggregated_transaction":
                query = f"SELECT Transaction_type, COUNT(*) as Count FROM aggregated_transaction where year={select_Year} AND quater = {select_Quater} GROUP BY Transaction_type"
                df = pd.read_sql_query(query, engine)
                # Extracting data from DataFrame
                Transaction_type = df['Transaction_type'].tolist()
                Count = df['Count'].tolist()
                # Construct the Pie chart using the retrieved data
                fig = go.Figure(data=[go.Pie(labels=Transaction_type, values=Count)])
                # Display the chart using Streamlit
                st.plotly_chart(fig)
            elif select_Type == "aggregated_user":
                query = f"SELECT Brand, COUNT(*) as Count FROM aggregated_user where year={select_Year}  AND quater = {select_Quater}  GROUP BY Brand "
                df = pd.read_sql_query(query, engine)
                Brand=df['Brand'].tolist()
                Count=df['Count'].tolist()
                fig = go.Figure(data=[go.Pie(labels=Brand, values=Count)])
                st.plotly_chart(fig)
            elif select_Type == "map_transaction":
                query = f"SELECT District, Total_amount FROM map_transaction where year={select_Year}  AND quater = {select_Quater} "
                df = pd.read_sql_query(query, engine)
                District=df['District'].tolist()
                Total_amount=df['Total_amount'].tolist()
                fig = go.Figure(data=[go.Pie(labels=District, values=Total_amount)])
                st.plotly_chart(fig)
            elif select_Type == "map_user":
                query = f"SELECT District, RegisteredUsers FROM map_user where year={select_Year}  AND quater = {select_Quater} "
                df = pd.read_sql_query(query, engine)
                District=df['District'].tolist()
                RegisteredUsers=df['RegisteredUsers'].tolist()
                fig = go.Figure(data=[go.Pie(labels=District, values=RegisteredUsers)])
                st.plotly_chart(fig)
            elif select_Type == "top_district_transaction":
                query = f"SELECT District, Transaction_Count_Pin FROM top_district_transaction where year={select_Year}  AND quater = {select_Quater} "
                df = pd.read_sql_query(query, engine)
                District=df['District'].tolist()
                Transaction_Count_Pin=df['Transaction_Count_Pin'].tolist()
                fig = go.Figure(data=[go.Pie(labels=District, values=Transaction_Count_Pin)])
                st.plotly_chart(fig)
            elif select_Type == "top_pincode_transaction":
                query = f"SELECT State, Transaction_amount_Pin FROM top_pincode_transaction where year={select_Year}  AND quater = {select_Quater} "
                df = pd.read_sql_query(query, engine)
                State=df['State'].tolist()
                Transaction_amount_Pin=df['Transaction_amount_Pin'].tolist()
                fig = go.Figure(data=[go.Pie(labels=State, values=Transaction_amount_Pin)])
                st.plotly_chart(fig)
            elif select_Type == "top_user":
                query = f"SELECT State, RegisteredUsers FROM top_user where year={select_Year}  AND quater = {select_Quater} "
                df = pd.read_sql_query(query, engine)
                State=df['State'].tolist()
                RegisteredUsers=df['RegisteredUsers'].tolist()
                fig = go.Figure(data=[go.Pie(labels=State, values=RegisteredUsers)])
                st.plotly_chart(fig)
        if select_graph == "Bar":
            Bar_Plot(select_Year, select_Quater, select_Type)
        elif select_graph =="Pie":
            Pie_plot(select_Year, select_Quater, select_Type)

      
         
    
 
        




