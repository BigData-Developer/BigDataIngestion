# BigDataIngestion
## 📌 Project: Scalable Big Data Ingestion with Dynamic Chunking and Delta Lake Storage

### 📝 Overview
Designed and implemented an automated Big Data ingestion pipeline in Databricks to extract over **500 million records daily** from a PostgreSQL source. The pipeline supports **dynamic chunking** based on data volume (yearly, quarterly, monthly, or daily) to avoid memory overload and ensures efficient **incremental loading** into **Delta Lake tables** stored in **Azure Data Lake Storage (ADLS)**.

---

### 🔧 Tech Stack
- **Databricks (PySpark)**
- **Azure Data Lake Storage Gen2 (ADLS)**
- **Delta Lake**
- **Azure Key Vault** (for credential decryption)
- **PostgreSQL** (source system)
- **Spark SQL**, **Base64**, **Email Alerts**

### ⚙️ Key Features
- **Auto-scaling ingestion:** Loads full table only when under threshold; otherwise chunks by year/quarter/month/day.
- **Watermark-based incremental load:** Uses last load timestamp stored in a config table.
- **Secure Credential Management:** Passwords decrypted from Azure Key Vault.
- **Dynamic path generation:** File paths created dynamically by database, table, and date.
- **Robust Logging:** Custom log collection with centralized inserts.
- **Format-aware Writing:** Writes to Delta for high-volume tables, Parquet for specific business domains.

---

### 📊 Performance Optimizations
- Coalescing writes to reduce file fragmentation
- Predicate pushdown on watermark columns
- Dynamic shuffle partition handling
- Broadcast join avoided via data chunking

---

### 📧 Monitoring
- Email alerts for:
  - Missing watermark column
  - Failure in job execution per table

---

### 🧠 How to Use (Execution Steps)
1. Configure source database & table info in the `configurations.configvalues` table by executing `insert_Postgres_Configs.py` notebook.
2. Set single batch data threshold via notebook widget (`singlebatchdatalimit`).
3. Trigger the notebook in Databricks with necessary widgets and permissions.
4. Review logs and Delta tables in ADLS for successful ingestion.

---

### 📈 Outcome
- Supports scalable ingestion for tables with **hundreds of millions of records**.
- Reduces failure risk by breaking large loads into manageable chunks.
- Allows **auditable, repeatable, and secure** data pipeline execution.

---

*For a fully working example, refer to the `PostgresToDataLake_Ingestion_Notebook.py` file.*

