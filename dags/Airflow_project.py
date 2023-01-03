# import thư viện
import csv
from airflow import DAG
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import psycopg2


#tạo list newspaper web 1
def check_newspaper_web_1():
    list_newspaper_old =[]
    try:
        with open('/home/airflow/cafef.csv','r',encoding='utf-8') as f:
            reader = csv.reader(f,delimiter=';')
            for row in reader:
                Title,Link,author,time,category,web = row
                list_newspaper_old.append(Title[1:-1])
    except FileNotFoundError :
        list_newspaper_old =[]
    return list_newspaper_old
# lấy dữ liệu web 1
def Craw_data_web_1():
    try:
        list_=check_newspaper_web_1()
        link_cafef="https://cafef.vn"
        url = "https://cafef.vn/thi-truong-chung-khoan.chn"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        Element = soup.findAll('li',class_='tlitem clearfix')
        file=open('/home/airflow/cafef.csv','wb')
        i=0
        for e in Element:
            title = e.find('a').attrs['title']
            title = title.replace('"','')
            title = title.replace("'","")
            title = title.replace(',','')
            if title in list_:
                continue
            else:
                file.write('{'.encode() + title.encode() +'}'.encode()+ ';'.encode())
                link = e.find('a').attrs['href']
                L=link_cafef.encode() + link.encode()
                file.write('{'.encode() + L+'}'.encode()+ ';'.encode())
                res=requests.get(L)
                sou=BeautifulSoup(res.content,"html.parser")
                try:
                    author=sou.find('p',class_='author').text
                except:
                    author="không xác định"
                try:
                    time=sou.find('span', class_="pdate").text
                except:
                    time="không xác định"
                try:
                    category=sou.find('a', class_="cat").text
                except:
                    category ="chứng khoán"
                file.write('{'.encode()+' '.encode() + author.encode() +'}'.encode() + ';'.encode())
                file.write('{'.encode() + time.encode()+'}'.encode() + ';'.encode())
                file.write('{'.encode() + category.encode() +'}'.encode()+ ';'.encode() +'{'.encode() + 'cafef'.encode() +'}'.encode() +'\n'.encode())
                i+=1
        file.close
        return i
    except:
        file=open('/home/airflow/cafef.csv','wb')
        file.write("{Trang web 1 không có báo mới};{Web 1 does not have a new newspaper  !};{};{};{};{}".encode())
        file.close()
        return 0
#insert data web 2
def import_data_web_1():
    #connect database
    conn = psycopg2.connect("dbname='postgres' user='airflow' host='host.docker.internal' password='airflow'")
    #import data
    with open('/home/airflow/cafef.csv', 'r', encoding='utf-8') as f:
    # Create a CSV reader
        reader = csv.reader(f,delimiter=';')

        # Iterate through the rows of the CSV file
        for row in reader:
            print(row)
        # Extract the values from the row
            Title,Link,author,time,category,web = row

            # Construct the INSERT statement
            sql = f"INSERT INTO List_article (Title,Link,author,time,category,web) VALUES ('{Title}', '{Link}', '{author}', '{time}', '{category}','{web}')"

        # Execute the INSERT statement
            with conn.cursor() as cursor:
                cursor.execute(sql)

    # Commit the transaction
    conn.commit()

        # Close the connection
    conn.close()
    return True
#tạo list newspaper web 2
def check_newspaper_web_2():
    list_newspaper_old =[]
    try:
        with open('/home/airflow/baodautu.csv','r',encoding='utf-8') as f:
            reader = csv.reader(f,delimiter=';')
            for row in reader:
                Title,Link,author,time,category,web  = row
                list_newspaper_old.append(Title[1:-1])
    except FileNotFoundError:
        list_newspaper_old =[]
    return list_newspaper_old
#lấy dữ liệu web 2
def Craw_data_web_2():
    try:
        list_=check_newspaper_web_2()
        url = "https://baodautu.vn/tai-chinh-chung-khoan-d6/"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        Element = soup.findAll('div',class_='desc_list_news_home')
        file=open('/home/airflow/baodautu.csv','wb')
        i=0
        for e in Element:
            title =e.find('a').text
            title = title.replace('"','')
            title = title.replace("'","")
            title = title.replace(';','')
            if title in list_:
                continue
            else:
                file.write('{'.encode() + title.encode() +'}'.encode() + ';'.encode())
                the_loai = e.find('a',class_="cl_green").text
                link = e.find('a').attrs['href']
                file.write('{'.encode() + link.encode() +'}'.encode() + ';'.encode())
                res=requests.get(link)
                sou = BeautifulSoup(res.content,"html.parser")
                author=sou.find('a',class_="author cl_green").text
                file.write('{'.encode()+' '.encode()+author.encode()+'}'.encode()+ ';'.encode())
                time = sou.find('span',class_="post-time").text
                file.write('{'.encode()+time[2:].encode()+'}'.encode() + ';'.encode())
                file.write('{'.encode()+the_loai.encode()+'}'.encode()+ ';'.encode() +'{'.encode() + 'baodautu'.encode() +'}'.encode() + '\n'.encode())
                i+=1
        file.close
        return i
    except:
        file=open('/home/airflow/baodautu.csv','wb')
        file.write("{Trang web 2 không có báo mới};{Web 2 does not have a new newspaper !};{};{};{};{}".encode())
        file.close()
        return 0
#đưa dữ liệu lên database
def import_data_web_2():
    #connect database
    conn = psycopg2.connect("dbname='postgres' user='airflow' host='host.docker.internal' password='airflow'")
    #import data
    with open('/home/airflow/baodautu.csv', 'r', encoding='utf-8') as f:
    # Create a CSV reader
        reader = csv.reader(f,delimiter=';')

        # Iterate through the rows of the CSV file
        for row in reader:
        # Extract the values from the row
            Title,Link,author,time,category,web = row

            # Construct the INSERT statement
            sql = f"INSERT INTO List_article (Title,Link,author,time,category,web) VALUES ('{Title}', '{Link}', '{author}', '{time}', '{category}','{web}')"

        # Execute the INSERT statement
            with conn.cursor() as cursor:
                cursor.execute(sql)

    # Commit the transaction
    conn.commit()

        # Close the connection
    conn.close()
    return True
#tạo list newspaper web 3
def check_newspaper_web_3():
    list_newspaper_old =[]
    try:
        with open('/home/airflow/kinhtechungkhoan.csv','r',encoding='utf-8') as f:
            reader = csv.reader(f,delimiter=';')
            for row in reader:
                print(row)
                Title,Link,author,time,category,web = row
                list_newspaper_old.append(Title[1:-1])
    except FileNotFoundError:
        list_newspaper_old =[]
    return list_newspaper_old
#lấy dữ liệu web 3
def Craw_data_web_3():
    try:
        list_=check_newspaper_web_3()
        url = "https://kinhtechungkhoan.vn/chung-khoan"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        Element = soup.findAll('a',class_='box-img')
        file=open('/home/airflow/kinhtechungkhoan.csv','wb')
        i=0
        for e in range(0,10):
            title = Element[e].attrs['title']
            title = title.replace('"','')
            title = title.replace("'","")
            title = title.replace(',','')
            if title in list_:
                continue
            else:
                file.write('{'.encode() + title.encode() +'}'.encode() + ';'.encode())
                link = Element[e].attrs['href']
                file.write('{'.encode() + link.encode() +'}'.encode()+ ';'.encode())
                res = requests.get(link)
                sou = BeautifulSoup(res.content, "html.parser")
                author_ = sou.findAll('strong')
                author=author_[-1].text
                file.write('{'.encode()+' '.encode()+author.encode()+'}'.encode()+';'.encode())
                time_d=sou.find('span',class_='format_date').text
                time_t=sou.find('span',class_='format_time').text
                file.write('{'.encode()+time_d.encode()+' '.encode()+time_t.encode()+'}'.encode()+';'.encode())
                the_loai = sou.find('a', class_="acted").text
                file.write('{'.encode()+the_loai.encode()+'}'.encode()+ ';'.encode() +'{'.encode() + 'kinhtechungkhoan'.encode() +'}'.encode()+'\n'.encode())
                i+=1
        file.close
        return i
    except:
        file=open('/home/airflow/kinhtechungkhoan.csv','wb')
        file.write("{Trang web 3 không có báo mới};{Web 3 does not have a new newspaper  !};{};{};{};{}".encode())
        file.close()
        return 0
#đưa dữ liệu lên database
def import_data_web_3():
    #connect database
    conn = psycopg2.connect("dbname='postgres' user='airflow' host='host.docker.internal' password='airflow'")
    #import data
    with open('/home/airflow/kinhtechungkhoan.csv', 'r', encoding='utf-8') as f:
    # Create a CSV reader
        reader = csv.reader(f,delimiter=';')

        # Iterate through the rows of the CSV file
        for row in reader:
        # Extract the values from the row
            Title,Link,author,time,category,web = row

            # Construct the INSERT statement
            sql = f"INSERT INTO List_article (Title,Link,author,time,category,web) VALUES ('{Title}', '{Link}', '{author}', '{time}', '{category}','{web}')"

        # Execute the INSERT statement
            with conn.cursor() as cursor:
                cursor.execute(sql)

    # Commit the transaction
    conn.commit()

        # Close the connection
    conn.close()
    return True
#tạo list newspaper web 4
def check_newspaper_web_4():
    list_newspaper_old =[]
    try:
        with open('/home/airflow/stockbiz.csv','r',encoding='utf-8') as f:
            reader = csv.reader(f,delimiter=';')
            for row in reader:
                Title,Link,author,time,category,web = row
                list_newspaper_old.append(Title[1:-1])
    except FileNotFoundError:
        list_newspaper_old =[]
    return list_newspaper_old
#lấy dữ liệu web 4
def Craw_data_web_4():
    try:
        list_=check_newspaper_web_4()
        url = "https://www.stockbiz.vn/Default.aspx"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        Element = soup.findAll('div',class_='latest_news_text')
        file=open('/home/airflow/stockbiz.csv','wb')
        i=0
        for e in Element:
            title = e.find('a').text
            title = title.replace('"','')
            title = title.replace("'","")
            title = title.replace(',','')
            if title in list_:
                continue
            else:
                file.write('{'.encode() + title.encode() +'}'.encode() + ';'.encode())
                link = e.find('a').attrs['href']
                file.write('{'.encode() + link.encode() +'}'.encode() + ';'.encode())
                res=requests.get(link)
                sou=BeautifulSoup(res.content,"html.parser")
                file.write('{'.encode()+' không xác định'.encode()+'}'.encode() + ';'.encode())
                time=sou.find('span', class_="news_date").text
                time_=time[10:-8]
                file.write('{'.encode()+time_.encode()+'}'.encode() + ';'.encode())
                file.write('{'.encode()+'Chứng khoán'.encode()+'}'.encode()+ ';'.encode() +'{'.encode() + 'stockbiz'.encode() +'}'.encode() + '\n'.encode())
                i+=1
        file.close
        return i
    except:
        file=open('/home/airflow/stockbiz.csv','wb')
        file.write("{Trang web 4 không có báo mới};{Web 2 does not have a new newspaper  !};{};{};{};{}".encode())
        file.close()
        return 0
#đưa dữ liệu lên database
def import_data_web_4():
    #connect database
    conn = psycopg2.connect("dbname='postgres' user='airflow' host='host.docker.internal' password='airflow'")
    #import data
    with open('/home/airflow/stockbiz.csv', 'r', encoding='utf-8') as f:
    # Create a CSV reader
        reader = csv.reader(f,delimiter=';')

        # Iterate through the rows of the CSV file
        for row in reader:
        # Extract the values from the row
            Title,Link,author,time,category,web = row

            # Construct the INSERT statement
            sql = f"INSERT INTO List_article (Title,Link,author,time,category,web) VALUES ('{Title}', '{Link}', '{author}', '{time}', '{category}','{web}')"

        # Execute the INSERT statement
            with conn.cursor() as cursor:
                cursor.execute(sql)

    # Commit the transaction
    conn.commit()

        # Close the connection
    conn.close()
    return True
#kiểm tra có báo mới hay không
def check_newspaper_new():
    number_web1=Craw_data_web_1()
    number_web2=Craw_data_web_2()
    number_web3=Craw_data_web_3()
    number_web4=Craw_data_web_4()
    if number_web1==number_web2==number_web3==number_web4==0:
        return "no_new_newspaper"
    else:
        return "create_table_main"
#báo cáo nếu không có báo mới  
def no_new_newspaper_f():
    import smtplib,ssl
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    # Create an EmailMessage object
    msg = MIMEMultipart()

    # Set the subject, sender, and recipient
    msg['Subject'] = 'Latest newpapers'
    msg['From'] = 'hoainhat8866@gmail.com'
    msg['To'] = 'hoainhat8866@gmail.com'
    sender_email = "hoainhat8866@gmail.com"
    password = "ZUZYeqwopntzyzsl"
    text="Chưa có báo mới"

    message = MIMEText(text) 
    msg.attach(message)   

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(msg['From'], msg['To'], msg.as_string())
        server.quit() 
    return True
#phân tích list báo mới
def hot_stock_():
    number_newspaper=Craw_data_web_1()+Craw_data_web_2()+Craw_data_web_3()+Craw_data_web_4()
    paths=['/home/airflow/cafef.csv','/home/airflow/baodautu.csv','/home/airflow/kinhtechungkhoan.csv','/home/airflow/stockbiz.csv']
    dfs=[]
    for path in paths:
        df=pd.read_csv(path,names=['title', 'Link','author','time','category','web'],sep=";")
        columns = ['title', 'Link','author','time','category','web']
        for col in columns:
            df[col] = df[col].str[1:-1]
        dfs.append(df)#
    df=pd.concat(dfs)
    df1=pd.concat(dfs)
    df2=pd.concat(dfs)
    df['Ma'] = df['title'].str.extract(r'([ |(][A-Z]{3}[ |)])')
    df=df['Ma'].str[1:-1]
    df_Ma=df.dropna()
    counts = df_Ma.value_counts() 
    most_common = counts.idxmax()
    pattern=r'{}'.format(most_common)
    df_hot=df1[df1['title'].str.contains(pattern)]
    with open("/home/airflow/data.csv",'wb') as f:
        f.write(f"Mã chứng khoán được quan tâm nhiều nhất là {most_common} \n".encode())
        f.write(f"Những bài báo liên quan đến {most_common} \n".encode())
        csv_hot = df_hot.to_csv(header=0,index=False)
        f.write(csv_hot.encode() + '\n'.encode())
        f.write(f"Có {number_newspaper} bài báo mới trong 1 giờ qua \n".encode())
        csv_string=df2.to_csv(header=0,index=False)
        f.write(csv_string.encode())
        f.close()
    return True
#gửi email báo cáo
def email():
    import smtplib,ssl
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    # Create an EmailMessage object
    msg = MIMEMultipart()

    # Set the subject, sender, and recipient
    msg['Subject'] = 'Latest newpapers'
    msg['From'] = 'hoainhat8866@gmail.com'
    msg['To'] = 'hoainhat8866@gmail.com'
    sender_email = "hoainhat8866@gmail.com"
    password = "ZUZYeqwopntzyzsl"

    csv_file = open("/home/airflow/data.csv", "r",encoding="utf-8")
    csv_reader = csv.reader(csv_file)
    csv_contents = ""

    for row in csv_reader:
        csv_contents += ",".join(row) + "\n"
    csv_file.close()

    message = MIMEText(csv_contents) 
    msg.attach(message)   

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(msg['From'], msg['To'], msg.as_string())
        server.quit() 
    return True
#hàm chính
dag = DAG(
    'Final_Project',
    default_args={
        'email': ['hoainhat8866@gmail.com'],
        'email_on_failure': True,
    },
    description='Stock futures',
    schedule=timedelta(hours=1),
    start_date= datetime.today(),
    tags=['FX16197'])
#tạo bảng lưu trữ
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='Postges_conn',
    sql="""
    BEGIN;
    DROP TABLE IF EXISTS list_article;
    CREATE TABLE if not exists list_article (
        ID SERIAL PRIMARY KEY ,
        Title TEXT,
        Link TEXT,
        author TEXT,
        time TEXT,
        category TEXT,
        web TEXT);
    COMMIT;""",
    dag=dag
    )
check1 = PythonOperator(
    task_id = 'check_newspaper_web_1',
    python_callable=check_newspaper_web_1,
    dag=dag
    )
check2 = PythonOperator(
    task_id = 'check_newspaper_web_2',
    python_callable=check_newspaper_web_2,
    dag=dag
    )
check3 = PythonOperator(
    task_id = 'check_newspaper_web_3',
    python_callable=check_newspaper_web_3,
    dag=dag
    )
check4 = PythonOperator(
    task_id = 'check_newspaper_web_4',
    python_callable=check_newspaper_web_4,
    dag=dag
    )

craw_web_1 = PythonOperator(
    task_id = 'Craw_data_wed_1',
    python_callable=Craw_data_web_1,
    dag=dag
    )
craw_web_2 = PythonOperator(
    task_id = 'Craw_data_wed_2',
    python_callable=Craw_data_web_2,
    dag=dag
    )
craw_web_3= PythonOperator(
    task_id = 'Craw_data_wed_3',
    python_callable=Craw_data_web_3,
    dag=dag
    )
craw_web_4 = PythonOperator(
    task_id = 'Craw_data_wed_4',
    python_callable=Craw_data_web_4,
    dag=dag
    )
import_web_1 = PythonOperator(
    task_id = 'import_data_web_1',
    python_callable=import_data_web_1,
    dag=dag
    )
import_web_2 = PythonOperator(
    task_id = 'import_data_web_2',
    python_callable=import_data_web_2,
    dag=dag
    )
import_web_3 = PythonOperator(
    task_id = 'import_data_web_3',
    python_callable=import_data_web_3,
    dag=dag
    )
import_web_4 = PythonOperator(
    task_id = 'import_data_web_4',
    python_callable=import_data_web_4,
    dag=dag
    )
check_newspaper_ne=BranchPythonOperator(
    task_id='check_newspaper_new',
    python_callable=check_newspaper_new,
    dag=dag
)
no_new_newspaper=PythonOperator(
    task_id='no_new_newspaper',
    python_callable=no_new_newspaper_f,
    dag=dag
)
#tạo bảng chính
create_table_main = PostgresOperator(
    task_id='create_table_main',
    postgres_conn_id='Postges_conn',
    sql="""
    CREATE TABLE IF NOT EXISTS author(
    id SERIAL primary key,
    author text UNIQUE);

    CREATE TABLE IF NOT EXISTS category(
    id SERIAL primary key,
    category text UNIQUE);

    CREATE TABLE IF NOT EXISTS web(
    id SERIAL primary key,
    web text UNIQUE);

    CREATE TABLE IF NOT EXISTS fact_paper(
    ID SERIAL primary key,
    title text,
    link text,
    time text,
    id_author Integer,
    id_category integer,
    id_web integer,
    FOREIGN KEY (id_author) REFERENCES author(id),
    FOREIGN KEY (id_category) REFERENCES category(id),
    FOREIGN KEY (id_web) REFERENCES web(id)
    )""",
    dag=dag
    )
#etl data
etl_data = PostgresOperator(
    task_id='etl_data',
    postgres_conn_id='Postges_conn',
    sql="""
    INSERT INTO category (category)
    SELECT DISTINCT category
    FROM list_article
    ON CONFLICT DO NOTHING;

    INSERT INTO author (author)
    SELECT DISTINCT author
    FROM list_article
    ON CONFLICT DO NOTHING;

    INSERT INTO web (web)
    SELECT DISTINCT web
    FROM list_article
    ON CONFLICT DO NOTHING;

    INSERT INTO fact_paper (title, link,time,id_author,id_category,id_web)
    SELECT title,link,time,author.id,category.id,web.id
    FROM list_article 
    join author on list_article.author::text = author.author::text
    join category on list_article.category::text = category.category::text
    join web on list_article.web::text = web.web::text;""",
    dag=dag
    )
hot_stock =PythonOperator(
    task_id = 'hot_stock',
    python_callable=hot_stock_,
    dag=dag
    )
email_=PythonOperator(
    task_id = 'email',
    python_callable=email,
    dag=dag
    )

create_table>>[check1,check2,check3,check4]
check1>>craw_web_1>>import_web_1>>check_newspaper_ne
check2>>craw_web_2>>import_web_2>>check_newspaper_ne
check3>>craw_web_3>>import_web_3>>check_newspaper_ne
check4>>craw_web_4>>import_web_4>>check_newspaper_ne
check_newspaper_ne>>[create_table_main,no_new_newspaper]
create_table_main>>etl_data>>hot_stock>>email_
