# Flink Python Jobs

Thư mục này chứa các Flink jobs được viết bằng Python, tương đương với các Java jobs trong thư mục `flink-jobs`.

## Cấu trúc thư mục

```
flink-jobs-python/
├── src/
│   └── jobs/
│       ├── word_count_job.py          # Word count job
│       ├── customer_analytics_job.py   # Customer analytics job  
│       └── customer_debezium_job.py    # Customer CDC job
├── requirements.txt                    # Python dependencies
├── run_job.py                         # Script để chạy jobs
└── README.md                          # Hướng dẫn này
```

## Các Jobs có sẵn

### 1. Word Count Job (`word_count_job.py`)
- Đếm số lần xuất hiện của từng từ trong text
- Tương đương với `WordCountJob.java`
- Sử dụng dữ liệu static để demo

### 2. Customer Analytics Job (`customer_analytics_job.py`)
- Phân tích số lượng khách hàng được tạo mỗi phút
- Tương đương với `CustomerAnalyticsJob.java`
- Sử dụng datagen connector để tạo dữ liệu mock

### 3. Customer Debezium Job (`customer_debezium_job.py`)
- Đọc CDC data từ Kafka sử dụng Debezium format
- Tương đương với `CustomerDebeziumJob.java`
- Tính số khách hàng được tạo mỗi 30 giây

## Cách chạy Jobs

### Phương pháp 1: Sử dụng Docker Compose (Khuyến nghị)

1. **Khởi động toàn bộ infrastructure:**
```bash
cd docker
docker-compose up -d
```

2. **Kiểm tra trạng thái services:**
```bash
docker-compose ps
```

3. **Truy cập Flink Web UI:**
- Java Flink: http://localhost:8081
- Python Flink: http://localhost:8082

4. **Chạy Python job trong container:**
```bash
# Vào container Python Flink
docker-compose exec jobmanager-python bash

# Chạy job
python /opt/flink/python-jobs/run_job.py word_count
python /opt/flink/python-jobs/run_job.py customer_analytics
python /opt/flink/python-jobs/run_job.py customer_debezium
```

### Phương pháp 2: Chạy trực tiếp với Python (Local Development)

1. **Cài đặt dependencies:**
```bash
cd flink-jobs-python
pip install -r requirements.txt
```

2. **Chạy job:**
```bash
python run_job.py word_count
python run_job.py customer_analytics
python run_job.py customer_debezium
```

### Phương pháp 3: Submit job qua Flink CLI

1. **Vào container:**
```bash
docker-compose exec jobmanager-python bash
```

2. **Submit job:**
```bash
flink run -py /opt/flink/python-jobs/src/jobs/word_count_job.py
flink run -py /opt/flink/python-jobs/src/jobs/customer_analytics_job.py
flink run -py /opt/flink/python-jobs/src/jobs/customer_debezium_job.py
```

## Yêu cầu hệ thống

- Docker & Docker Compose
- Python 3.8+ (nếu chạy local)
- Apache Flink 1.18.1
- Kafka (cho Debezium job)
- PostgreSQL (cho CDC data)

## Troubleshooting

### Lỗi thường gặp:

1. **ModuleNotFoundError: No module named 'pyflink'**
   ```bash
   pip install apache-flink==1.18.1
   ```

2. **Kafka connection refused**
   - Đảm bảo Kafka service đang chạy: `docker-compose ps kafka`
   - Kiểm tra health check: `docker-compose logs kafka`

3. **Python job không chạy trong container**
   - Kiểm tra Python path: `echo $PYTHONPATH`
   - Verify file permissions: `ls -la /opt/flink/python-jobs/`

### Debug logs:

```bash
# Xem logs của Python Flink cluster
docker-compose logs jobmanager-python
docker-compose logs taskmanager-python

# Xem logs của job cụ thể
docker-compose exec jobmanager-python flink list
docker-compose exec jobmanager-python flink log <job-id>
```

## So sánh với Java Jobs

| Tính năng | Java | Python |
|-----------|------|--------|
| Performance | Cao hơn | Thấp hơn nhưng đủ dùng |
| Development Speed | Chậm hơn | Nhanh hơn |
| Ecosystem | Rộng hơn | Đang phát triển |
| Debugging | IDE support tốt | Print-based debugging |
| Deployment | JAR files | Python scripts |

## Mở rộng

Để thêm job mới:

1. Tạo file `.py` trong `src/jobs/`
2. Implement function `main()`
3. Thêm mapping trong `run_job.py`
4. Update README này

## Tài liệu tham khảo

- [PyFlink Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/python/overview/)
- [Flink Python API](https://nightlies.apache.org/flink/flink-docs-release-1.18/api/python/)
- [Apache Flink Examples](https://github.com/apache/flink/tree/master/flink-python/pyflink/examples)
