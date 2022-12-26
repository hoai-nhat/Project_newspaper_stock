FROM apache/airflow:2.4.3
RUN python -m pip install --upgrade pip
RUN pip install matplotlib
RUN python -m pip install secure-smtplib
RUN pip install vnstock
RUN pip install openpyxl
RUN pip install tensorflow
RUN pip install scikit-learn

