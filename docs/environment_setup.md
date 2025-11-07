# 🧩 Hadoop Sandbox Environment Configuration (Health Risk Analysis)

This file defines all key access details, environment variables, directory structures, and configuration notes for working with the Hadoop sandbox and the **Health Risk Analysis** project dataset.

It assumes the project repository is extracted/cloned to:
```bash
/root/health-risk-analysis
```
(If you place it somewhere else, update `PROJECT_ROOT` accordingly.)

---

## 🔐 Access & Login Setup

### 1. SSH Access (Sandbox Terminal)
Use this to log into the Hadoop Sandbox terminal from your local machine or PuTTY:

```bash
ssh root@127.0.0.1 -p 2222
```
**Default credentials:**
- **Username:** root  
- **Password:** hadoop

Once logged in, you’ll be in the sandbox Linux environment where you can run Hadoop commands like:
```bash
hdfs dfs -ls /
```

---

### 2. Web UI Access
You can monitor Hadoop services and the cluster state through your browser:

| Service | URL |
|----------|-----|
| Ambari (HDP) | http://127.0.0.1:8080 |
| Hue (if installed) | http://127.0.0.1:8888 |
| Namenode UI | http://127.0.0.1:50070 |
| ResourceManager | http://127.0.0.1:8088 |

---

### 3. File Transfer via FileZilla
To move files between your host computer and Hadoop Sandbox:

| Setting | Value |
|----------|--------|
| Host | 127.0.0.1 |
| Port | 22 *(or 2222 depending on setup)* |
| Protocol | SFTP - SSH File Transfer Protocol |
| Logon Type | Normal |
| User | root |
| Password | hadoop |

Once connected, navigate to `/root/` on the remote side and create/upload files to:
```text
/root/health_risk_data/
```
You can then move files from this local sandbox path into HDFS.

---

## 📁 Directory Structure

### Local (Sandbox Linux)

These paths reflect your **actual repo** layout from `health-risk-analysis.zip`:

| Path | Purpose |
|------|----------|
| `/root/health_risk_data/` | Local staging folder where data is uploaded from FileZilla |
| `/root/health-risk-analysis/` | Project root (git repository) |
| `/root/health-risk-analysis/data/raw/` | Raw unprocessed data files |
| `/root/health-risk-analysis/data/processed/` | Cleaned or transformed data |
| `/root/health-risk-analysis/hive/` | Hive queries and scripts |
| `/root/health-risk-analysis/mapreduce/` | MapReduce programs |
| `/root/health-risk-analysis/spark/` | Spark notebooks / scripts |
| `/root/health-risk-analysis/reports/` | Reports / outputs |
| `/root/health-risk-analysis/docs/` | Project documentation (including this file) |

### Hadoop Distributed File System (HDFS)

| Path | Purpose |
|------|----------|
| `/data/` | Root directory for project data in HDFS |
| `/data/health_risk/` | Folder for the Health Risk Analysis dataset in HDFS |
| `/data/health_risk/smoking_driking_dataset.csv` | Uploaded dataset file in HDFS |

---

## ⚙️ Environment Variables

Add these to your `.bashrc` or `.bash_profile` to make them persistent across sessions:

```bash
# Hadoop Sandbox Environment Variables
export HDFS_DATA_PATH=/data/health_risk
export LOCAL_DATA_PATH=/root/health_risk_data
export HADOOP_USER_NAME=root

# Project root (matches the repo folder name)
export PROJECT_ROOT=/root/health-risk-analysis

# Optional helpers (uncomment if you want)
# export RAW_DATA_PATH=$PROJECT_ROOT/data/raw
# export PROCESSED_DATA_PATH=$PROJECT_ROOT/data/processed

# Hadoop & Hive client locations (typical for HDP sandbox)
export HIVE_HOME=/usr/hdp/current/hive-client
export HADOOP_HOME=/usr/hdp/current/hadoop-client
export PATH=$PATH:$HADOOP_HOME/bin:$HIVE_HOME/bin
```

Then apply changes with:
```bash
source ~/.bashrc
```

If you cloned the repo to a different location (e.g. `/home/hadoop/health-risk-analysis`), change `PROJECT_ROOT` to match that path.

---

## 🧠 Useful Commands

### HDFS Operations
```bash
# List contents in HDFS project folder
hdfs dfs -ls /data/health_risk

# Upload a file from local Linux to HDFS
hdfs dfs -put /root/health_risk_data/smoking_driking_dataset.csv /data/health_risk/

# Check file size in HDFS
hdfs dfs -du -h /data/health_risk
```

### Hive Integration
```sql
-- Example to create external table in Hive
CREATE EXTERNAL TABLE health_risk_raw (
  col1 STRING,
  col2 STRING
  -- TODO: replace with real column names and types
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/health_risk/';
```

---

## 🚀 Verification Steps

1. Run `jps` → ensure NameNode, DataNode, ResourceManager, NodeManager are active.  
2. Verify HDFS file: `hdfs dfs -ls /data/health_risk`.  
3. Confirm environment variables with `echo $HDFS_DATA_PATH` and `echo $PROJECT_ROOT`.  
4. Check that the repo exists at `$PROJECT_ROOT` and contains `data/`, `hive/`, `mapreduce/`, `spark/`, `docs/`, `reports/`.  
5. If all good → proceed with Hive/Spark analysis.

---

## ✅ Summary

| Item | Status |
|------|--------|
| HDFS directory created | ✅ `/data/health_risk` |
| File uploaded | ✅ `smoking_driking_dataset.csv` in HDFS |
| Local staging directory | ✅ `/root/health_risk_data` |
| Project root aligned | ✅ `/root/health-risk-analysis` |
| Environment variables configured | ✅ (after updating `.bashrc`) |
| Hive ready | ⚙️ pending schema definition |
| Spark ready | ⚙️ pending notebooks / jobs |
