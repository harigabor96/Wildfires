# Wildfires

The purpose of this project is to show my skills in modern data engineering and data architecture with Apache Spark, Scala, and Azure Databricks, and to serve as a source of best practices for myself. The architecture described here is my own work and it tries to reconcile my first-hand experience with traditional data warehousing practices like Ralph Kimball’s and Bill Inmon’s works and with newly emerging practices, most notably Maxime Beauchemin’s Functional Data Engineering.

## The Architecture
This design pattern is expected to be general-purpose which means that:
- It should support any type of analysis (BI, ML, AI, Ad-Hoc, etc.).
- It should support streaming and real-time analytics (as well as batch processing).
- It should have scalable ETL performance.
- It should have scalable code complexity.
- It should not lose data or create data errors.
- It should not be affected by late arriving data.

![alt text](https://github.com/harigabor96/Wildfires/blob/main/resources/Architecture.jpg?raw=true)

### Independent Data Marts
The independent data mart approach was labeled as an anti-pattern both by Inmon and Kimball despite its' popularity. The reasoning behind this was that a Data Warehouse should not contain contradictory information. This effectively means that incorrect but consistent ETL is preferred over divergent ETL that runs on the same source data, which leads to the concept of the Enterprise Data Warehouse, the "single source of truth". However in my opinion there is no better single source of truth than a Data Lake because one can make sure that it hasn't been touched by any buggy ETL... This effectively means that the monolithic horror of the EDW can be entirely replaced by modular Data Marts (Silver, Gold, Databricks SQL) that depend only on the Data Lake (Raw Zone, Bronze Zone).

### Granularity and Late Arriving Data
The main idea behind this architecture originates from Inmon, in a way that data should be persisted at the lowest granularity, which makes late arriving data, creating data errors by aggregation (and then re-aggregating the entire historic dataset), and losing data non-issues. Of course, this means query time aggregations, which only work when a powerful query engine like Photon (Databricks SQL) is present as the final layer of the architecture.

### Partition Pruning
One of the issues of traditional architectures was that some operations that are necessary from a business point of view (deduplication, row updates) involve reading the whole historic dataset in the Data Warehouse. In a partitioned dataset, this can be mitigated by choosing the partitions carefully and making sure that partition pruning is in effect when possible.

### Snapshot/Archive Schema
The schema for the final persistence layer (Gold Zone) is my own creation and it draws from OLTP database design (specifically the posting mechanism of ERP systems) and functional programming. My key observation here was that data present in the source is either:
- A closed record, that is never changed again, which means it’s immutable.
-	An open record, that is subject to changes, which means it’s mutable.

My solution is to represent this duality in the Data Lakehouse:
-	Closed records should be treated as Archives.
-	Open records should be treated as Snapshots.
-	Tables with mixed records should be separated. When it’s not possible to do so, they should either be treated as a Snapshot or an updatable Archive.

### Aggregation and Integration
The final element of the architecture is a powerful query engine (Photon) which lets the user create aggregations and integration efficiently. Here, ad-hoc queries can be written and executed, an analytics-specific schema (Snowflake, Star Schema, etc.) can be applied, tables can be joined, unioned, aggregated, etc.

## The Project
The scope of the project is nothing special as it only transforms and visualizes one source table containing historical geographical data of US Wildfires. 

The source dataset can be found at:

[https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires](https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires)

### Components
 - The "WildfiresETL" Scala project applies all the above rules to the ETL process as well as a clean project structure and consistent naming conventions. 
 - The storage folder contains the "sample" version of Data Lakehouse with a raw zone containing .csv files in a folder structure and a curated zone containing the usual delta tables.
 - The .pbix report contains a very simple report to visualize the output data. This report is without errors but isn't optimized according to BI best practices, as I don't have a way to simulate Databricks SQL.
 
![alt text](https://github.com/harigabor96/Wildfires/blob/main/resources/FireTimeTravel.PNG?raw=true)

### WIP/Backlog
 - Pipeline selection with command line parameter
 - Unit Tests
 - Proper column types
 - General code formatting
