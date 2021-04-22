#!/usr/bin/env python
# coding: utf-8

# Data Scraping and Analysis Project

# For this project, I would like to collect and analyze data from Google Trends on the trends of a certain list of keywords that were searched on Google in 2019 and 2020 in Vietnam. This project has different stages to it that formulate a complete code program:
# 1. I created an excel file that contains the keywords that I wanted get data on. There are a total of 65 keywords divided into 7 topics.
#     
# 2. Using the pytrends package, I found the data on the number of searches of these keywords on Google Trends in 2019 and 2020 and assign them to a new excel file. 
#     
# 3. I also put the data I gathered into the Postgresql database system for accessible and convenient data management.
#     
# 4. I performed some analysis on the top 5 and top 10 keywords that were searched the most in 2019 and 2020. I also found the most searched keyword in each month of 2019 and 2020 and executed visualization on this.
# 
# The final result of this project is to create a program that asks for a user's input of an excel file that contains certain keywords that they want to collect data on on Google Trends, save the data into a Postgresql database, and perform data visualization of the analysis of that data. 

# In[1]:


import pandas as pd
import numpy as np
from pytrends.request import TrendReq
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
#Import necessary Python packages to execute data collection,data storage, data analysis, and data visualization


# In[2]:


#this is a function that accepts an excel file name or path and returns a dataframe for this file 
def get_data(xlsx_file_path):
    df = pd.read_excel(xlsx_file_path)
    special_char = [ '/' , '*' , '?' , ':' , '[' , ']'] 
    for col in list(df.columns): 
        for char in special_char:
            if char in col:
                df.rename(columns={col:col.replace(char,'_')},inplace=True) #if there are any special characters
                #in the column names, delete the special characters from the column names
    for col in list(df.columns):
        if ' ' in col:
            df.rename(columns={col:col.replace(' ','')},inplace=True) #delete any spaces in the column names
    return df


# In[4]:


#this is a function takes the name of an excel file containing the keywords and the timeframe during which we want
#to see the trends as inputs and returns a dataframe that has the number of searches of each keywords monthly 
def get_trend(name,timeframe): 
    dataset=[]
    pytrend = TrendReq(hl='en-US',tz=420)
    for ele in kt[name]: 
        if str(ele) != 'nan':
            keyword = [ele] 
            pytrend.build_payload(keyword, cat=0, timeframe=timeframe,geo='VN')
            data = pytrend.interest_over_time()
            if not data.empty:
                data = data.drop(labels='isPartial',axis=1)
                dataset.append(data)
                df = pd.concat(dataset,axis=1) 
    return df


# In[1]:


#this is a function that takes a dataframe that contains the trends of the keywords and the timeframe during which 
#we want to see the trends as inputs and returns a dictionary that has the number of searches of each 
#keyword monthly 
def dict_trend(df,timeframe):
    dic = {}
    for col in df.columns:
        df1 = get_trend(col,timeframe)
        dic[col] = df1
    return dic


# In[6]:


#this is a function that creates a new dataframe that has the columns: "keyword", "date", "value", and "trend_type"
#and the data for this dataframe is taken from the dictionary containing trends of the keywords
def to_dbtable(dic, dic_key_lst):
    new_df = pd.DataFrame()
    for key in dic_key_lst:
        for col in list(dic[key].columns):
            sub_df = dic[key][[col]].reset_index()
            sub_df.insert(0, 'keyword', col) 
            sub_df.rename(columns={col:'value'},inplace=True)
            sub_df.insert((len(sub_df.columns)-1), 'trend_type',key)
            new_df = new_df.append(sub_df)
    return new_df


# In[7]:


#create two tables in PostgreSQL for the 2019 trends and the 2020 trends
def connect1(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
        commands = (
        """
        CREATE TABLE vn_trending20 (
            ID SERIAL PRIMARY KEY,
            keyword TEXT NOT NULL,
            date DATE,
            value INT,
            trend_type TEXT   
        )
        """,
        """
        CREATE TABLE vn_trending19 (
            ID SERIAL PRIMARY KEY,
            keyword TEXT NOT NULL,
            date DATE,
            value INT,
            trend_type TEXT   
        )
        """)
        cur = conn.cursor()
        # create the table
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn


# In[8]:


#insert the data of the trends of the keywords into the two 2019 and 2020 tables
def execute_many(conn, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of lists from the dataframe values
    lst = [list(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO {} ({}) VALUES(%s,%s,%s,%s)".format(table,cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, lst)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()


# In[9]:


#connect to the PostgreSQL database server
def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn, cur = None, None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
        # create a cursor
        cur = conn.cursor()
        print('Connection complete')
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while excuting SQL" + error)

    return conn, cur


# In[10]:


#find the top ten most searched keywords from both tables 
def top_ten_trending(params_dic):
    sql = """
            SELECT keyword, sum(value) AS sum_val
            FROM 
            (SELECT keyword, value FROM vn_trending19 
            UNION ALL 
            SELECT keyword, value FROM vn_trending20) AS top_trend
            GROUP BY keyword
            ORDER BY sum_val DESC
            LIMIT 10
            ;"""
    conn, cur = connect(params_dic)
    cur.execute(sql)
    rd = cur.fetchall()
    conn.close()
    cur.close()
    df = pd.DataFrame(rd,
                          columns=['keyword', 'sum_value',])
    writer = pd.ExcelWriter('vn_trending_top_ten.xlsx')
    df.to_excel(writer, index=False)
    print("Xuất thành công báo cáo Top ten trending")
    writer.save()


# In[266]:


#merge two tables into one and export the new merged table to an excel file
def top_trending(params_dic):
    sql = """
            SELECT keyword, value, date FROM vn_trending19
            UNION ALL 
            SELECT keyword, value, date FROM vn_trending20
            ;"""
    conn, cur = connect(params_dic)
    cur.execute(sql)
    rd = cur.fetchall()
    conn.close()
    cur.close()
    df = pd.DataFrame(rd,
                      columns=['keyword', 'value','date'])
    writer = pd.ExcelWriter('vn_trending_1920.xlsx')
    df.to_excel(writer, index=False)
    print("Xuất thành công báo cáo Top ten trending 2")
    writer.save()


# In[12]:


#export the 2020 trending table from PostgreSQL to an excel file
def top_trend20(params_dic):
    sql = """
            SELECT keyword, date, value, trend_type FROM vn_trending20
            ;"""
    conn, cur = connect(params_dic)
    cur.execute(sql)
    rd = cur.fetchall()
    conn.close()
    cur.close()
    df = pd.DataFrame(rd,
                      columns=['keyword','date','value','trend_type'])
    writer = pd.ExcelWriter('vn_trending20.xlsx')
    df.to_excel(writer, index=False)
    print("Xuất thành công báo cáo vn_trending20")
    writer.save()


# In[13]:


#export the 2019 trending table from PostgreSQL to an excel file
def top_trend19(params_dic):
    sql = """
            SELECT keyword, date, value, trend_type FROM vn_trending19
            ;"""
    conn, cur = connect(params_dic)
    cur.execute(sql)
    rd = cur.fetchall()
    conn.close()
    cur.close()
    df = pd.DataFrame(rd,
                      columns=['keyword','date','value','trend_type'])
    writer = pd.ExcelWriter('vn_trending19.xlsx')
    df.to_excel(writer, index=False)
    print("Xuất thành công báo cáo vn_trending19")
    writer.save()


# In[2]:


def to_pivot(dic): #create a pivot table for each topic in the original excel file 
    big_lst= {}
    key_lst = list(dic.keys())
    for key in key_lst: #iterate through the list of topics and the dataframe corresponding to each topic
        df = dic[key]
        df['date'] = df['date'].dt.strftime('%m')
        df['date'] = pd.to_numeric(df['date'])
        pivot = df.pivot_table(index='keyword',columns='date', aggfunc='sum')
        pivot.columns = pivot.columns.droplevel(0)
        pivot.index.name='Keyword' 
        for col in pivot.columns:
            pivot.rename(columns={col: 'Month'+ ' '+ str(col)},inplace=True) 
            #change the names of the columns
        pivot = pivot.reset_index()
        int_lst = []
        for i in range(1,len(pivot)+1):
            int_lst.append(i)
        pivot.insert(0,'STT',int_lst) 
        #add the 'STT' column to the big pivot table of each of the 7 topics
        big_lst[key] = pivot
        #add the big pivot table corresponding to each topic to a dictionary called "big_lst" in which 
        #the topics are the keys
    return big_lst


# In[19]:


#requires the user to input some essential information of their database for future connection with the database
print('Please enter some database information: ')
host = str(input('Insert host name: '))
database = str(input('Insert database name: '))
user = str(input('Insert user name: '))
password = str(input('Insert password: '))


# The code below shows what this program will execute if the user asks it to do a predetermined task. A menu will appear with the numbered tasks for the user to choose. There are 7 tasks in total:
# 
# 1. Get the information about the trends of certain keywords from the original file: If the user asks the program to do this task, the program will asks the user to enter the name or the full file path of the excel file that contains the keywords the user wants to find the trends of and the timeframe(s) during they want to see the trends. The program will try to find that excel file from the local drive and if there exists such a file, it will start finding fetching the number of searches of each keyword from Google Trends and save the data into the table(s) in the PostgresSQL database. 
# 
# 2. Export the report of the top 10 most searched keywords: If the user wants to execute this task, the program will find the top 10 most searched keywords in the entire database along with their total number of searches throughout the years and the month and year the keywords were searched the most. The data will then be exported to an excel file called "top_ten_trending.xlsx".
# 
# 3. Export the report of the information of the keywords by topic and by month in 2020: If this task chosen to be executed, the program will gather the information of the number of searches of each keyword by month in 2020. The information will then be saved into an excel file called "search_keyword_in_2020.xlsx".
# 
# 4. Export a line chart of the top 5 most searched keywords in 2020: For this task, the program will export a PNG file called "top_search_key_2020.png" of a line chart depicting the number of searches of the top 5 most searched keywords in 2020. 
# 
# 5. Export a bar chart of the top 5 most searched keywords in 2019: For this task, the program will export a PNG file called "top_search_key_2019.png" of a bar chart depicting the number of searches of the top 5 most searched keywords in 2019. 
# 
# 6. Find and display the information of the top 5 most searched keywords in both 2020 and 2019: If the user wants to execute this task, the program will find the top 5 most searched keywords in 2019 and the top 5 most searched keywords in 2020 along with their total number of searches in that year and the month and year the keywords were searched the most. The data will then be exported to an excel file called "top_five_trending_1920.xlsx".
# 
# 7. Exit: If the user chooses the "Exit" opiton, the program stop running.  

# In[ ]:


program_running = True
while program_running:
    
    print (30 * '-')
    print ("    O P T I O N  M E N U")
    print (30 * '-')
    print('1. Get the information about the trends of certain keywords from the original file')
    print('2. Export the report of the top 10 most searched keywords')
    print('3. Export the report of the information of the keywords by topic and by month in 2020')
    print('4. Export a line chart of the top 5 most searched keywords in 2020')
    print('5. Export a bar chart of the top 5 most searched keywords in 2019')
    print('6. Find and display the information of the top 5 most searched keywords in both 2020 and 2019')
    print('...')
    print('99. Exit')
    
    choice = input('Please enter a number from [1,2,3,4,5,6,99]: ')
    choice = int(choice)
    
    if choice == 99:
        program_running = False
        break
    
    else:
        if choice == 1:
            try:
                excel_file_path = str(input("Enter excel file name or file path: "))    
                kt = get_data(excel_file_path)
                print('Import data successfully')
            except: 
                print('The file path is not correct or the file does not exist')
            
            # number of elements 
            n = int(input("Enter the number of elements : ")) 
            # Below line read inputs from user using map() function  
            tf = list(map(str,input("\nEnter the timeframes (Enter the most recent timeframe first): ")
                          .strip().split(',')))[:n]
            
            dic_lst = []
            for timeframe in tf:
                dic = dict_trend(kt,timeframe)
                dic_lst.append(dic)            

            df_lst = []
            for dic in dic_lst: 
                dic_key_lst = list(dic.keys())
                new_df = to_dbtable(dic, dic_key_lst)
                df_lst.append(new_df)

            param_dic = {"host" : host, "database":database, "user":user, "password":password}
            conn = connect1(param_dic)
            execute_many(conn, df_lst[0], 'vn_trending20')
            execute_many(conn, df_lst[1], 'vn_trending19')
            
            print('The required data is successfully fetched and saved into the database')
            
            print (30 * '-')
            print ("    O P T I O N  M E N U")
            print (30 * '-')
            print('1. Get the information about the trends of certain keywords from the original file')
            print('2. Export the report of the top 10 most searched keywords')
            print('3. Export the report of the information of the keywords by topic and by month in 2020')
            print('4. Export a line chart of the top 5 most searched keywords in 2020')
            print('5. Export a bar chart of the top 5 most searched keywords in 2019')
            print('6. Find and display the information of the top 5 most searched keywords in both 2020 and 2019')
            print('...')
            print('99. Exit')
            
            
        elif choice == 2:
            param_dic = {"host" : host, "database":database, "user":user, "password":password}
            conn = connect(param_dic)
            top_ten_trending(param_dic)
            top_trending(param_dic)

            top10 = get_data('vn_trending_top_ten.xlsx')
            all_kw = get_data('vn_trending_1920.xlsx')

            all_kw['month_year'] = all_kw['date'].dt.strftime('%m/%y')

            all_kw_gb = all_kw.pivot_table(index='month_year',columns='keyword',aggfunc = 'sum')
            all_kw_gb.columns = all_kw_gb.columns.droplevel(0)

            total_df = pd.DataFrame()
            for col in list(all_kw_gb.columns):
                df = all_kw_gb[all_kw_gb[col] == all_kw_gb[col].max()][[col]]
                df = df.reset_index()
                df['keyword'] = col
                df.rename(columns={col:'val'},inplace=True)
                total_df = total_df.append(df)

            total_df = total_df.drop('val',axis=1)
            new_df = pd.merge(top10, total_df, on='keyword', how='left')

            int_lst = []
            for i in range(1, len(new_df)+1):
                int_lst.append(i)
            new_df.insert(0,'STT',int_lst)

            new_df.rename(columns={'keyword':'Keyword','sum_value':'Total Search Count','date':'Date with the most searches'},
                     inplace=True)
            new_df

            new_df.to_excel('top_10_trending.xlsx',index=False)

            print('top_10_trending.xlsx is successfully exported')
            
            print (30 * '-')
            print ("    O P T I O N  M E N U")
            print (30 * '-')
            print('1. Get the information about the trends of certain keywords from the original file')
            print('2. Export the report of the top 10 most searched keywords')
            print('3. Export the report of the information of the keywords by topic and by month in 2020')
            print('4. Export a line chart of the top 5 most searched keywords in 2020')
            print('5. Export a bar chart of the top 5 most searched keywords in 2019')
            print('6. Find and display the information of the top 5 most searched keywords in both 2020 and 2019')
            print('...')
            print('99. Exit')
            
            
        elif choice == 3:
            param_dic = {"host" : host, "database": database, "user":user, "password":password}
            top_trend20(param_dic)

            df = get_data('vn_trending20.xlsx')
            trendtype = list(df['trend_type'].unique())
            trendtype_dic = {}
            for trend in trendtype:
                new_df = df[df['trend_type'] == trend][['keyword','date','value']]
                trendtype_dic[trend] = new_df

            trend_dic = to_pivot(trendtype_dic)

            w = pd.ExcelWriter('search_keyword_in_2020.xlsx')
            for key in list(trend_dic.keys()):
                df = trend_dic[key]
                df.to_excel(w, sheet_name = key,index=False)
            w.save()
            
            print('search_keyword_in_2020.xlsx is successfully exported')
            
            print (30 * '-')
            print ("    O P T I O N  M E N U")
            print (30 * '-')
            print('1. Get the information about the trends of certain keywords from the original file')
            print('2. Export the report of the top 10 most searched keywords')
            print('3. Export the report of the information of the keywords by topic and by month in 2020')
            print('4. Export a line chart of the top 5 most searched keywords in 2020')
            print('5. Export a bar chart of the top 5 most searched keywords in 2019')
            print('6. Find and display the information of the top 5 most searched keywords in both 2020 and 2019')
            print('...')
            print('99. Exit')

        
        if choice == 4:
            param_dic = {"host" : host, "database": database, "user":user, "password":password}
            top_trend20(param_dic)

            df = get_data('vn_trending20.xlsx')

            groupby_df = df.groupby('keyword').sum()
            groupby_df = groupby_df.reset_index()
            top_5 = groupby_df.nlargest(5,'value')

            sns.set_style('whitegrid')

            fig, ax = plt.subplots(figsize=(10,6))
            plot1 = sns.lineplot(data= top_5, x='keyword',y='value', ax = ax)
            plot1.set_xlim(['Jack','Buồn Làm Chi Em Ơi'])
            plot1.set_ylim([1800,3400])
            ax.set_title('TOP 5 MOST SEARCHED KEYWORDS IN VIETNAM 2020')
            plt.savefig('top_search_key_2020.png')

            print('The line chart of the top 5 most searched keywords in 2020 is successfully graphed')
            print('The line chart is successfully exported as a PNG called file top_search_key_2020.png')
            
            print (30 * '-')
            print ("    O P T I O N  M E N U")
            print (30 * '-')
            print('1. Get the information about the trends of certain keywords from the original file')
            print('2. Export the report of the top 10 most searched keywords')
            print('3. Export the report of the information of the keywords by topic and by month in 2020')
            print('4. Export a line chart of the top 5 most searched keywords in 2020')
            print('5. Export a bar chart of the top 5 most searched keywords in 2019')
            print('6. Find and display the information of the top 5 most searched keywords in both 2020 and 2019')
            print('...')
            print('99. Exit')        
            
            
        if choice == 5:
            param_dic = {"host" : host, "database": database, "user":user, "password":password}
            top_trend19(param_dic)

            df = get_data('vn_trending19.xlsx')

            groupby_df = df.groupby('keyword').sum()
            groupby_df = groupby_df.reset_index()
            top_5 = groupby_df.nlargest(5,'value')

            sns.set_style('whitegrid')

            fig, ax = plt.subplots(figsize=(10,6))
            plot2 = sns.barplot(data= top_5, x='keyword', y='value', ax = ax)
            plot2.set_ylim([0,4000])
            ax.set_title('TOP 5 MOST SEARCHED KEYWORDS IN VIETNAM 2019')
            plt.savefig('top_search_key_2019.png')

            print('The bar chart of the top 5 most searched keywords in 2019 is successfully graphed')
            print('The bar chart is successfully exported as a PNG called file top_search_key_2019.png') 
            
            print (30 * '-')
            print ("    O P T I O N  M E N U")
            print (30 * '-')
            print('1. Get the information about the trends of certain keywords from the original file')
            print('2. Export the report of the top 10 most searched keywords')
            print('3. Export the report of the information of the keywords by topic and by month in 2020')
            print('4. Export a line chart of the top 5 most searched keywords in 2020')
            print('5. Export a bar chart of the top 5 most searched keywords in 2019')
            print('6. Find and display the information of the top 5 most searched keywords in both 2020 and 2019')
            print('...')
            print('99. Exit')    
            
            
        if choice == 6:
            param_dic = {"host" : host, "database": database, "user":user, "password":password}
            top_trend20(param_dic)
            top_trend19(param_dic)

            df20 = get_data('vn_trending20.xlsx')
            df19 = get_data('vn_trending19.xlsx')

            df_dic = {'2020': df20, '2019': df19}

            top_five_dic = {}
            for key in list(df_dic.keys()):
                df = df_dic[key]
                groupby_df = df.groupby('keyword').sum()
                groupby_df = groupby_df.reset_index()
                top_5 = groupby_df.nlargest(5,'value')
                top_five_dic[key] = top_5

            top_five20 = top_five_dic['2020']

            df20['month_year'] = df20['date'].dt.strftime('%m-%y')
            gb1 = df20.pivot_table(index='month_year',columns='keyword',aggfunc = 'sum')
            gb1.columns = gb1.columns.droplevel(0)

            total_df1 = pd.DataFrame()
            for col in list(gb1.columns):
                df = gb1[gb1[col] == gb1[col].max()][[col]]
                df = df.reset_index()
                df['keyword'] = col
                df.rename(columns={col:'val'},inplace=True)
                total_df1 = total_df1.append(df)

            total_df1 = total_df1.drop('val',axis=1)
            new_df1 = pd.merge(top_five20, total_df1, on='keyword', how='left')

            new_df1.rename(columns={'keyword':'Keyword','value':'Total Search Count',
                                    'month_year':'Date with the most searches'}, inplace=True)

            int_lst = []
            for i in range(1, len(new_df1)+1):
                int_lst.append(i)
            new_df1.insert(0,'STT',int_lst)

            columns = pd.MultiIndex.from_product([['Year 2020'],
                                                  ['No.', 'Keyword', 'Total Search Count','Date with the most searches']])
            new_df1.columns = columns

            print('The top 5 most searched keywords in 2020 is successfully found')


            top_five19 = top_five_dic['2019']

            df19['month_year'] = df19['date'].dt.strftime('%m-%y')
            gb2 = df19.pivot_table(index='month_year',columns='keyword',aggfunc = 'sum')
            gb2.columns = gb2.columns.droplevel(0)

            total_df2 = pd.DataFrame()
            for col in list(gb2.columns):
                df = gb2[gb2[col] == gb2[col].max()][[col]]
                df = df.reset_index()
                df['keyword'] = col
                df.rename(columns={col:'val'},inplace=True)
                total_df2 = total_df2.append(df)

            total_df2 = total_df2.drop('val',axis=1)
            new_df2 = pd.merge(top_five19, total_df2, on='keyword', how='left')

            new_df2.rename(columns={'keyword':'Keyword','value':'Total Search Count',
                                    'month_year':'Date with the most searches'}, inplace=True)

            columns = pd.MultiIndex.from_product([['Year 2019'],
                                                  ['Keyword', 'Total Search Count','Date with the most searches']])
            new_df2.columns = columns

            print('The top 5 most searched keywords in 2019 is successfully found')

    
            final_df = pd.concat([new_df1, new_df2],axis=1)
            final_df.to_excel('top_five_trending_1920.xlsx')

            print('The information of the top 5 most searched keywords 2019 and 2020 is successfully exported as an excel file called top_five_trending_1920.xlsx')   
            
            print (30 * '-')
            print ("    O P T I O N  M E N U")
            print (30 * '-')
            print('1. Get the information about the trends of certain keywords from the original file')
            print('2. Export the report of the top 10 most searched keywords')
            print('3. Export the report of the information of the keywords by topic and by month in 2020')
            print('4. Export a line chart of the top 5 most searched keywords in 2020')
            print('5. Export a bar chart of the top 5 most searched keywords in 2019')
            print('6. Find and display the information of the top 5 most searched keywords in both 2020 and 2019')
            print('...')
            print('99. Exit') 
        
        elif choice not in [1,2,3,4,5,6,99]:
            print('Please only choose from the option menu')

