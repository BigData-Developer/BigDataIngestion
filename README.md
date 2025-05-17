# BigDataIngestion
Designed and implemented an automated Big Data ingestion pipeline in Databricks to extract over 500 million records daily from a PostgreSQL source. The pipeline supports dynamic chunking based on data volume (yearly, quarterly, monthly, or daily) to avoid memory overload and ensures efficient incremental loading into Delta Lake tables stored in Azure Data Lake Storage (ADLS).
