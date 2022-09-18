# Wildfires

The purpose of this project is to show my skills in modern data engineering and data architecture with Apache Spark, Scala, and Azure Databricks, and to serve as a source of best practices for myself. The architecture described here is my own work and it tries to reconcile my first-hand experience with traditional data warehousing practices like Ralph Kimball’s and Bill Inmon’s works and with newly emerging practices, most notably Maxime Beauchemin’s Functional Data Engineering.
## The Architecture
![alt text](https://github.com/harigabor96/Wildfires/blob/main/Architecture.jpg?raw=true)

This design pattern is expected to be general-purpose which means that:
- It should support any type of analysis (BI, ML, AI, Ad-Hoc, etc.).
- It should support streaming and real-time analytics (as well as batch processing).
- It should have scalable ETL performance.
- It should have scalable code complexity.
- It should not lose data or create data errors.
- It should not be affected by late arriving data.

In its’ current state, the architecture is able to satisfy all of the above criteria, provided that a powerful query engine like Photon (Databricks SQL) supports it. The main idea behind the architecture originates from Inmon, in a way that data should only be persisted at the lowest (and even lower) granularity, which makes late arriving data a non-issue. 

The schema for the final persistence layer (Gold Zone) is my own creation and it draws from OLTP database design (specifically the posting mechanism of ERP systems) and functional programming. My key observation here was that data present in the source is either:
- A closed transaction, that is never changed again, which means it’s immutable.
-	An open transaction, that is subject to changes, which means it’s mutable.

My solution is to represent this duality in the Data Lakehouse:
-	Closed transactions should be treated as Archives.
-	Open transactions should be treated as Snapshots.
-	Tables with mixed transactions should be separated. When it’s not possible to do so, they should either be treated as a Snapshot or an updateable Archive.

Partially because of this, I decided to prefix the tables of the Gold Zone either as Snap_ or Arch_ rather than a Dim_ or a Fact_ and name this pattern the Snapshot/Archive Schema.

The final element of the architecture is a powerful query engine (Photon) and an advanced query editor (Databricks SQL) which lets the user create aggregations and integration efficiently. Here Dimensions and Facts can be extracted, indexed, joined, etc. from the Snapshot and Archive tables to create datasets that are ready for analysis.
## The Project
The scope of the project is nothing special as it only transforms and visualizes one source table containing historical geographical data of US Wildfires. 

The source dataset can be found at:
[https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires](https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires)

### Components
 - The "WildfiresETL" Scala project applies all the above rules to the ETL process as well as a clean project structure and consistent naming conventions. 
 - The storage folder contains the "sample" version of Data Lakehouse with a raw zone containing .csv files in a folder structure and a curated zone containing the usual delta tables.
 - The .pbix report contains a very simple report to visualize the output data. This report is without errors but isn't optimized according to BI best practices, as I don't have a way to simulate Databricks SQL.
 
![alt text](https://github.com/harigabor96/Wildfires/blob/main/FireTimeTravel.PNG?raw=true)

### WIP/Backlog
 - Pipeline selection with command line parameter
 - Unit Tests
 - Proper column types
 - General code formatting
