# Sử dụng Python 3.10 slim làm base image
FROM python:3.10-slim


# Thiết lập thư mục làm việc
WORKDIR /opt/dagster/app

# Sao chép và cài đặt các gói Python từ requirements.txt
COPY requirements.txt /opt/dagster/app
RUN pip install --upgrade pip && pip install -r requirements.txt

RUN apt-get update && apt-get install -y wget unzip && \
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb &&\
    apt install -y ./google-chrome-stable_current_amd64.deb &&\
    rm google-chrome-stable_current_amd64.deb && \
    apt-get clean
# Sao chép mã nguồn vào thư mục làm việc
COPY . /opt/dagster/app

# Khởi động Dagster bằng lệnh CMD
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]
