# import thư viện
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import numpy as np
import ssl
import os
import sys

#cung cấp quyền truy cập đến http
ssl._create_default_https_context = ssl._create_unverified_context

#sàn nghỉ ngày cuối tuần nên không cần báo cáo
def _out():
    dt = datetime.now()
    df =dt.weekday() 
    if df == 0 or df == 6 :
        return "end"
    return "clearfile"

#kiểm tra file để tránh việc ghi đè
def _clearfile():
    if os.path.exists("/home/airflow/VCB.csv") == True:
        os.remove("/home/airflow/VCB.csv") 
    return True

#lấy giá chứng khoán về trên vnstock cụ thể là mã VCB (Vietcombank)
def craw_stock_price(**kwargs):
    import vnstock

    CK = "VCB"
    #chạy hàng ngày
    to_day = kwargs["to_date"]
    df = vnstock.stock_historical_data(symbol=CK, start_date="2000-01-01", end_date=to_day)
    df.to_csv("/home/airflow/VCB.csv", index=None , header='true')
    return True
#đưa dữ liệu lên database để lưu trữ
def import_data_():
    import psycopg2

    #connect database
    conn = psycopg2.connect("dbname='stock' user='airflow' host='host.docker.internal' password='airflow'")
    cur = conn.cursor()
    #import data
    sql = "COPY VCB FROM stdin WITH CSV HEADER DELIMITER as ','"
    with open("/home/airflow/VCB.csv", "r") as file:
        cur.copy_expert(sql, file)
        conn.commit()
        #cur.close()
    return True
# tạo tool dự đoán giá chứng khoán
def train_model():

    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import Dense, Dropout, LSTM
    import matplotlib.pyplot as plt
    from sklearn.preprocessing import MinMaxScaler

    #đọc dữ liệu
    df=pd.read_csv("/home/airflow/VCB.csv")

    #tạo thêm dữ liệu để phân tích
    #Khoản giữa của giá cao nhất và giá thấp nhất (mô hình cây nến)
    df['H-L'] = df['High'] - df['Low']
    #Khoản giữa của giá mở cữa và giá đóng cửa
    df['O-C'] = df['Open'] - df['Close']
    #giá trị trung bình lần lượt 7 ngày , 14 ngày , 21 ngày
    ma_1 = 7
    ma_2 = 14
    ma_3 = 21
    df[f'SMA_{ma_1}'] = df['Close'].rolling(window=ma_1).mean()
    df[f'SMA_{ma_2}'] = df['Close'].rolling(window=ma_2).mean()
    df[f'SMA_{ma_3}'] = df['Close'].rolling(window=ma_3).mean()

    df[f'SD_{ma_1}'] = df['Close'].rolling(window=ma_1).std()
    df[f'SD_{ma_3}'] = df['Close'].rolling(window=ma_3).std()
    #xoá giá trị not a number trong moving average
    df.dropna(inplace=True)

    # process data
    pre_day = 60
    #xác định miền giá trị để máy học nhanh hơn
    scala_x = MinMaxScaler(feature_range=(0, 1))
    scala_y = MinMaxScaler(feature_range=(0, 1))
    #là những giá trị để dự đoán những ngày tiếp theo
    cols_x = ['H-L', 'O-C', f'SMA_{ma_1}', f'SMA_{ma_2}', f'SMA_{ma_3}', f'SD_{ma_1}', f'SD_{ma_3}']
    cols_y = ['Close']
    scaled_data_x = scala_x.fit_transform(df[cols_x].values.reshape(-1, len(cols_x)))
    scaled_data_y = scala_y.fit_transform(df[cols_y].values.reshape(-1, len(cols_y)))

    #chứu danh sách các mảng mỗi mảng sẽ chứa 60 giá đóng cửa liên tục
    x_total = []
    #danh sách giá đóng cửa của ngày hôm sau tương ứng với x_train
    y_total = []

    for i in range(pre_day, len(df)):
        x_total.append(scaled_data_x[i-pre_day:i])
        y_total.append(scaled_data_y[i])

    #số ngày test 
    test_size = 365
    
    #tách data thành 2 phần : 1 phần để train ; 1 phần để test
    x_train = np.array(x_total[:len(x_total)-test_size])
    x_test = np.array(x_total[len(x_total)-test_size:])
    y_train = np.array(y_total[:len(y_total)-test_size])
    y_test = np.array(y_total[len(y_total)-test_size:])

    #buil model
    #tạo lớp mạng cho dữ liệu đầu vào
    model = Sequential()
    # lớp LSTM mô tả thông tin đầu vào
    model.add(LSTM(units=60, return_sequences=True, input_shape=(x_train.shape[1], x_train.shape[2])))
    #lớp Dropout bỏ qua một số giá trị ngẫu nhiên để tránh mô hình học tủ 
    model.add(Dropout(0.2))
    model.add(LSTM(units=60, return_sequences=True))
    model.add(Dropout(0.2))
    model.add(LSTM(units=60, return_sequences=True))
    model.add(Dropout(0.2))
    model.add(LSTM(units=60, return_sequences=True))
    model.add(Dropout(0.2))
    model.add(LSTM(units=60))
    model.add(Dropout(0.2))
    #lớp out Dense đầu ra 1 chều để dự báo cho một giá trị
    model.add(Dense(units=len(cols_y)))
    #sữ dụng trình tối ưu hoá adam
    model.compile(optimizer='adam', loss='mean_squared_error')
    #huấn luyện với 120 lần lặp
    model.fit(x_train, y_train, epochs=120, steps_per_epoch=40, use_multiprocessing=True)
    #lưu model
    model.save("/home/airflow/VCB.h5")
    
    #dự đoán cho dữ liệu train
    predict_prices = model.predict(x_test)
    #đưa về giá trị gốc 
    predict_prices = scala_y.inverse_transform(predict_prices)

    #xữ lý giá thực
    real_price = df[len(df)-test_size:]['Close'].values.reshape(-1, 1)
    real_price = np.array(real_price)
    real_price = real_price.reshape(real_price.shape[0], 1)

    #tạo biểu đồ
    plt.plot(real_price, color="red", label="Real VCB Prices")
    plt.plot(predict_prices, color="blue", label="Predicted VCB Prices")
    plt.title("VCB Prices")
    plt.xlabel("Time")
    plt.ylabel("Stock Prices")
    plt.ylim(bottom=0)
    plt.legend()
    #lưu biểu đồ
    plt.savefig("/home/airflow/stock_futures.png")
    return True

#tạo tự động gửi email
def email():

    import smtplib, ssl
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.image import MIMEImage

    subject = "An stock report email"
    sender_email = "hoainhat8866@gmail.com"
    receiver_email = "hoainhat8866@gmail.com"
    password = "ZUZYeqwopntzyzsl"

    #Tạo tin nhắn nhiều phần và đặt tiêu đề
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    #Được đề xuất cho hàng loạt email
    message["Bcc"] = receiver_email 

    #Thêm nội dung vào email
    text = MIMEText('<img src="cid:image1">', 'html')
    message.attach(text)
    image = MIMEImage(open('/home/airflow/stock_futures.png', 'rb').read())
    image.add_header('Content-ID', '<image1>')
    message.attach(image)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email,message.as_string())

    return True

#hàm chính
dag = DAG(
    'Final_Project',
    default_args={
        'email': ['hoainhat8866@gmail.com'],
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Stock futures',
    schedule=timedelta(days=1),
    start_date= datetime.today() - timedelta(days=1),
    tags=['FX16197'])

#cuối tuần sàn không hoạt động
weekend = BranchPythonOperator(
        task_id = "weekend",
        python_callable=_out,
        dag=dag
)

#task end
end = DummyOperator(
    task_id = 'END',
    dag=dag
    )

#kiểm tra file tránh việc ghi đè 
clearfile = PythonOperator(
        task_id = 'clearfile',
        python_callable=_clearfile,
        dag=dag
    )

#task lấy data
crawl_data = PythonOperator(
    task_id='crawl_data',
    python_callable=craw_stock_price,
    op_kwargs={"to_date": "{{ ds }}"},
    dag=dag
)

#task tạo database
create_table_postgres_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='Postges_conn',
    sql=r"""
    DROP TABLE IF EXISTS VCB;
    CREATE TABLE if not exists VCB (
        Open varchar(10),
        High varchar(10),
        Low varchar(10),
        Close varchar(10),
        Volume varchar(10),
        TradingDate varchar(20)
    );
    """,
    dag=dag
)

#task đưa dữ liệu lên database
import_data= PythonOperator(
    task_id='imort_data',
    python_callable=import_data_,
    dag=dag
)

#task tạo mô hình dự đoán
train_model_ = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    dag=dag
)

#task gửi email báo cáo
email_operator = PythonOperator(
    task_id='email_operator',
    python_callable=email,
    dag=dag
)

weekend >> [clearfile,end]
clearfile >> crawl_data >> create_table_postgres_task >> import_data >> train_model_ >> email_operator >> end

