# RealEstate-ETL-Pipeline

![Pipeline Architecture](https://github.com/A7md-Waly/RealEstate-ETL-Pipeline/blob/main/RealEstate-ETL-Pipeline.png)

A production-ready ETL (Extract, Transform, Load) pipeline for Real Estate sales data analysis using Apache Airflow, Hadoop HDFS, and Apache Hive. This pipeline automates data processing workflows with Docker containerization for easy deployment and scalability.

## Architecture Overview

The pipeline orchestrates data flow through the following components:

- **Apache Airflow 2.10.3**: Workflow orchestration and scheduling with CeleryExecutor
- **Hadoop HDFS 3.3.6**: Distributed file storage system
- **Apache Hive 3.1.3**: Data warehousing with SQL-like queries
- **PostgreSQL 13**: Metadata storage for Airflow and Hive Metastore
- **Redis 7.2**: Message broker for Celery workers

## ETL Pipeline Workflow

1. **Extract**: Reads raw Real Estate sales data from CSV files
2. **Transform**: 
   - Removes `Residential Type` column
   - Cleans null values
   - Adds unique `SaleID` identifier
3. **Load**: 
   - Uploads processed data to HDFS
   - Creates Hive table with optimized schema
   - Loads data into Hive for analysis

## Project Structure

```
RealEstate-ETL-Pipeline/
├── dags/
│   └── real_state_ETL.py        # Main ETL pipeline DAG
├── docker-compose.yaml           # Multi-container orchestration
├── hadoop.env                    # Hadoop configuration
├── requirements.txt              # Python dependencies
└── .gitignore                   # Git exclusions
```

## Prerequisites

- Docker Engine 
- Docker Compose 
- Minimum 8GB RAM
- 20GB available disk space

## Quick Start

### 1. Clone Repository

```bash
git clone https://github.com/A7md-Waly/RealEstate-ETL-Pipeline.git
cd RealEstate-ETL-Pipeline
```

### 2. Create Required Directories

```bash
mkdir -p dags logs plugins config inputData outputData
```

### 3. Configure Environment

```bash
cat > .env << 'EOF'
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
_PIP_ADDITIONAL_REQUIREMENTS=hdfs requests
EOF
```

### 4. Prepare Data

Place your `Real_Estate_Sales.csv` file in the `inputData/` directory.

### 5. Initialize HDFS NameNode

```bash
docker compose run --rm -u root namenode bash -c "hdfs namenode -format && chown -R hadoop:hadoop /tmp/hadoop-hadoop"
```

### 6. Initialize HDFS DataNode

```bash
docker compose run --rm -u root datanode bash -c "mkdir -p /tmp/hadoop-hadoop/dfs/data && chown -R hadoop:hadoop /tmp/hadoop-hadoop"
```

### 7. Start Services

```bash
docker compose up -d
```

Wait approximately 2-3 minutes for all services to initialize.

### 8. Access Airflow UI

Navigate to `http://localhost:8080`
- **Username**: `airflow`
- **Password**: `airflow`

### 9. Create Hive Table (One-time Setup)

```bash
docker exec -it hiveserver2 /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
```

Execute in Beeline:

```sql
CREATE TABLE IF NOT EXISTS real_estate_sales (
    SaleID INT,
    List_Year INT,
    Town STRING,
    Address STRING,
    Assessed_Value DOUBLE,
    Sale_Amount DOUBLE,
    Sales_Ratio DOUBLE,
    Property_Type STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/real_estate/'
TBLPROPERTIES ('skip.header.line.count'='1');
```

Exit Beeline:
```sql
!quit
```

### 10. Run the Pipeline

1. In Airflow UI, locate the `real_estate_etl_pipeline` DAG
2. Toggle it to **ON**
3. Click **Trigger DAG** to execute manually
4. Monitor execution in the **Graph** or **Logs** view

## Service Endpoints

| Service | URL | Description |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | DAG management and monitoring |
| HDFS NameNode | http://localhost:9870 | HDFS cluster overview |
| YARN ResourceManager | http://localhost:8088 | YARN cluster manager |
| HiveServer2 Web UI | http://localhost:10002 | Hive service status |

## Data Schema

### Input CSV Format
```csv
List Year,Town,Address,Assessed Value,Sale Amount,Sales Ratio,Property Type,Residential Type
2006,Hartford,123 Main St,150000,200000,0.75,Residential,Single Family
```

### Output Hive Table Schema
```sql
SaleID          INT
List_Year       INT
Town            STRING
Address         STRING
Assessed_Value  DOUBLE
Sale_Amount     DOUBLE
Sales_Ratio     DOUBLE
Property_Type   STRING
```

## Troubleshooting

### Services Not Starting
```bash
# Check container status
docker compose ps

# View logs
docker compose logs [service_name]
```

### HDFS Connection Issues
```bash
# Verify namenode is running
docker compose logs namenode | tail -20

# Check HDFS health
docker exec namenode hdfs dfsadmin -report
```

### Hive Table Issues
```bash
# Verify file exists in HDFS
docker exec namenode hdfs dfs -ls /user/hive/warehouse/real_estate/

# Check Hive metastore
docker compose logs hive-metastore | grep -i "success\|error"
```

## Maintenance

### Stop All Services
```bash
docker compose down
```

### Clean Volumes (Reset Everything)
```bash
docker compose down -v
```

### View Service Logs
```bash
docker compose logs -f [service_name]
```

### Restart Specific Service
```bash
docker compose restart [service_name]
```

## Technologies Used

- **Orchestration**: Apache Airflow 2.10.3
- **Storage**: Hadoop HDFS 3.3.6
- **Data Warehouse**: Apache Hive 3.1.3
- **Databases**: PostgreSQL 13
- **Message Queue**: Redis 7.2
- **Containerization**: Docker & Docker Compose
- **Language**: Python 3.12

## Author

**Ahmed Waly**
- GitHub: [@A7md-Waly](https://github.com/A7md-Waly)
