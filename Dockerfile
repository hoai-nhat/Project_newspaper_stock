FROM apache/airflow:2.4.3
RUN python -m pip install --upgrade pip
RUN pip install vnstock
RUN pip install openpyxl
RUN pip install tensorflow
RUN pip install scikit-learn
RUN pip install matplotlib
RUN pip install pymssqlS
RUN python -m pip install secure-smtplib
RUN pip install apache-airflow-providers-microsoft-mssql
