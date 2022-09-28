# Wildfires
The purpose of this project is to show my skills in modern data engineering and data architecture with Apache Spark, Scala, and Azure Databricks, and to serve as a source of best practices for myself. The architecture described here is my own work and it tries to reconcile my first-hand experience with traditional data warehousing practices like Ralph Kimball’s and Bill Inmon’s works and with newly emerging practices, most notably Maxime Beauchemin’s Functional Data Engineering.
## The Project
The scope of this project is nothing special as its' main aim is to show quality, not quantity. The ETL pipelines transform one source table that contains historical geographical data of US Wildfires, which is then visualized by a Power BI report.

The source dataset can be found at:
[https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires](https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires)
### Components
 - The "WildfiresETL" Scala (and Spark) project contains the ETL pipelines. It has been built according to the design pattern described in "The Architecture" section and it has been unit and integration tested.
 - The storage folder contains the "sample" version of a Data Lakehouse with the raw zone containing .csv files in a folder structure and the curated zone containing the usual delta tables.
 - The .pbix file contains a simple Power BI report to visualize the output data.
 
![alt text](https://github.com/harigabor96/Wildfires/blob/main/resources/FireTimeTravel.PNG?raw=true)
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
The independent data mart approach was labeled as an anti-pattern both by Inmon and Kimball despite its' popularity. The reasoning behind this was that a Data Warehouse should not contain contradictory information. This effectively means that incorrect but consistent ETL is preferred over divergent ETL that runs on the same source data, which leads to the concept of the Enterprise Data Warehouse, the "single source of truth". 

However in a modern architecture there is no better single source of truth than a Data Lake because one can make sure that it hasn't been touched by any buggy ETL... This effectively means that the monolithic horror of the EDW can be entirely replaced by modular Data Marts (Silver, Gold, Databricks SQL) that depend only on the Data Lake (Raw Zone, Bronze Zone).
### Persistence at the Lowest Granularity
The main idea behind this architecture originates from Inmon, in a way that data should be persisted at the lowest granularity, which eliminates the problems that emerge from the combination of varying batch sizes, late-arriving data, and aggregation/windowing. It's worth noting that this design pattern allows lowering the granularity (explode) and storing tables of different grains separately (Gold Zone) to provide a flexible and clear structure for analysis.

Inmon also allowed persisting aggregations as long as the lowest grain data is also persisted, however considering the nature of streaming and how common late arriving data is, it is only viable when said aggregations are independent of batch size and result of commutative operations like addition. On top of this, maintaining idempotence with a foreachBatch pipeline can be quite tricky, so aggregation in the Gold Zone is best to be avoided.  
### Schema Separation
Just like aggregation, horizontal (join/merge) integration breaks when late arriving data is present as joining two tables via a pipeline would require the relevant batches of each data source to be available at the exact same time. Vertical integration (union/append) can be performed, however, it's not ideal as it would require two tables from separate sources to have the same schema. On top of this, the need might arise to separate the two tables in query time, which would mean filtering, which is more costly as an operation than union.

These problems can be easily solved by keeping the DAG of each Data Mart as a "Forest" where Bronze tables are the trunks and Gold tables are the topmost branches. When the separation (filtering) of tables with multiple business event/entity types into individual tables is added on top of this, the Gold Zone will provide maximum flexibility and performance for query time aggregation and integration, and also minimize storage and schema complexity.
### Partition Pruning
One of the issues of traditional architectures was that some operations that are necessary from a business point of view (deduplication, row updates) involve reading the whole historic dataset in the Data Warehouse. In a partitioned dataset, this can be mitigated by choosing the partitions carefully and making sure that partition pruning is in effect when possible.
### Snapshots and Archives
The schema for the final persistence layer (Gold Zone) is my own creation and it draws from OLTP database design (specifically the posting mechanism of ERP systems) and functional programming. My key observation here was that data present in the source is either:
- A closed record, that is never changed again, which means it’s immutable.
-	An open record, that is subject to changes, which means it’s mutable.

My solution is to represent this duality in the Data Lakehouse:
-	Closed records should be treated as Archives, that have unique natural/business PKs for the entire dataset.
-	Open records should be treated as Snapshots, that have unique natural/business PKs within each snapshot.
-	Tables with mixed records should be separated. When it’s not possible to do so, they should either be treated as Snapshots or an updatable Archive (again, foreachBatch idempotence!).
### Dynamic Aggregation and Integration
The final element of the architecture is a powerful query engine (Photon) which lets the user create aggregations and integration efficiently. Here, ad-hoc queries can be written and executed, an analytics-specific schema (Snowflake, Star, etc.) can be applied, tables can be joined, unioned, aggregated, etc. These operations are not necessarily re-calculated every time when a user executes a query as caching query results is supported by Databricks SQL.
