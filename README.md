# Wildfires
The purpose of this project is to show my skills in modern data engineering and data architecture with Apache Spark, Scala, and Databricks, and to serve as a source of best practices for myself. The architecture described here is my own work and it tries to reconcile my first-hand experience with traditional data warehousing practices like Ralph Kimball’s and Bill Inmon’s works, and with newly emerging practices like Data Mesh, Medallion Architecture, and Functional Data Engineering.
## The Project
The scope of this project is nothing special as its' main aim is to show quality, not quantity. The ETL pipelines transform one source table that contains historical geographical data of US Wildfires, which is then visualized by a Power BI report.

The source dataset can be found at:
[https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires](https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires)
### Components
 - The "etl" folder contains two Scala (and Spark) project modules which include the ETL pipelines. These have been built according to the design pattern described in "The Architecture" section and they have been unit and integration tested.
 - The storage folder contains the "sample" version of a Data Lakehouse with the raw zone containing .csv files in a folder structure and the curated zone containing the usual delta tables.
 - The .pbix file contains a simple Power BI report to visualize the output data.

 ### Dependencies
 If you want to run this app yourself, you have to download and locally install (with Maven) the compiled versions of:
 - [https://github.com/harigabor96/EzTL-Core/releases/tag/v1.0.1](https://github.com/harigabor96/EzTL-Core/releases/tag/v1.0.1)
 - [https://github.com/harigabor96/EzTL-IngestionTools/releases/tag/v1.0.1](https://github.com/harigabor96/EzTL-IngestionTools/releases/tag/v1.0.1)

![alt text](https://github.com/harigabor96/Wildfires/blob/main/resources/FireTimeTravel.PNG?raw=true)
## The Architecture
This design pattern is general-purpose which means that:
- It supports any type of analysis (BI, ML, AI, Ad-Hoc, etc.).
- It supports both streaming and batch processing.
- It has a scalable data model.
- It has scalable ETL performance.
- It has scalable project and code complexity.
- It does not lose data or create data errors by design.
- It is not affected by late arriving data.

The main influences behind it are:
- The Data Mesh Architecture: [https://www.datamesh-architecture.com/](https://www.datamesh-architecture.com/)
- The Medallion Architecture: [https://www.databricks.com/glossary/medallion-architecture](https://www.databricks.com/glossary/medallion-architecture)
- The Semantic Lakehouse: https://www.databricks.com/kr/dataaisummit/session/how-implement-semantic-layer-your-lakehouse 
- Functional Data Engineering: [https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a](https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a)

![alt text](https://github.com/harigabor96/Wildfires/blob/main/resources/Architecture.jpg?raw=true)
### Decentralization - Mesh between the Bronze Zone and Data Marts
The independent data mart approach was labeled as an anti-pattern both by Inmon and Kimball despite its' popularity. The reasoning behind this was that a Data Warehouse should not contain contradictory information. This effectively means that incorrect but consistent ETL is preferred over divergent ETL that runs on the same source data, which leads to the concept of the Enterprise Data Warehouse, the "single source of truth". 

However, in a modern architecture, there is no better single source of truth than raw data because one can make sure that it hasn't been touched by any buggy ETL... This effectively means that the monolithic horror of the EDW can be entirely replaced by **modular** and **consumer-aligned** Data Marts (Silver Zone, Gold Zone, Platinum Zone) that depend only on the **modular** and **source-aligned** ingested raw data (Bronze Zone).

### Democratization, Collaboration - Multiple Standardized Projects
The modularity should also be reflected in the code structure to avoid breaking pipelines that are already in production during updates and to provide a scalable codebase that can be worked on by multiple separate data teams. This is best done by creating shared utils, ingestion modules (Bronze), and datamarts (Silver, Gold) as separate projects, and updating and deploying them individually, preferably in a polyrepo structure (or in some cases monorepo, like this project).

It is also important to standardize the projects and the tooling (as the Data Mesh architecture suggests) in order to avoid the spread of bad practices and inefficient ETL. This is best done by creating separate projects for common utils and interfaces and importing them as dependencies into each ETL module via pom.xml. This specific project depends on the following standardization modules:
 - EzTL-Core for standardized ETL pipelines and app initialization: [https://github.com/harigabor96/EzTL-Core](https://github.com/harigabor96/EzTL-Core)
 - EzTL-IngestionTools for easy ingestion: [https://github.com/harigabor96/EzTL-IngestionTools](https://github.com/harigabor96/EzTL-IngestionTools)

### Preserving Data Quality - Persistence at the Lowest Granularity
The main idea behind this architecture originates from Inmon, in a way that data should be persisted at the lowest granularity, which eliminates the problems that emerge from the combination of varying batch sizes, late-arriving data, and aggregation/windowing. It's worth noting that this design pattern allows lowering the granularity (explode) and storing tables of different grains separately (Gold Zone) to provide a flexible and clear structure for analysis.

Inmon also allowed persisting aggregations as long as the lowest grain data is also persisted, however considering the nature of streaming and how common late arriving data is, it is only viable when said aggregations are independent of batch size and result of commutative operations like addition. On top of this, maintaining idempotence with a foreachBatch pipeline ([like this solution](https://towardsdatascience.com/idempotent-writes-to-delta-lake-tables-96f49addd4aa)) can be quite tricky, so aggregation in the Gold Zone is best to be avoided.  
### Flexibility of Consumption - Schema Separation
Just like aggregation, horizontal (join/merge) integration breaks when late arriving data is present as joining two tables via a pipeline would require the relevant batches of each data source to be available at the exact same time. Vertical integration (union/append) can be performed, however, it's not ideal as it would require two tables from separate sources to have the same schema. On top of this, the need might arise to separate the two tables in query time, which would mean filtering, which is more costly as an operation than union.

These problems can be easily solved by keeping the DAG of each Data Mart as a "Forest" where Bronze tables are the trunks and Gold tables are the topmost branches. When the separation (filtering) of tables with multiple business event/entity types into individual tables is added on top of this, the Gold Zone will provide maximum flexibility and performance for query time aggregation and integration, and also minimize storage and schema complexity.
### Scalable Performance - Partition Pruning
One of the issues of traditional architectures was that some operations that are necessary from a business point of view (deduplication, row updates) involve reading the whole historic dataset in the Data Warehouse. In a partitioned dataset, this can be mitigated by choosing the partitions carefully and making sure that partition pruning is in effect when possible.
### Handling Changing and Historical Data - Snapshots and Archives
The schema for the final persistence layer (Gold Zone) is my own creation and it draws from OLTP database design (specifically the posting mechanism of ERP systems) and functional programming/data engineering. My key observation here was that data present in the source is either:
- A closed record, that is never changed again, which means it’s immutable.
-	An open record, that is subject to changes, which means it’s mutable.

My solution is to represent this duality in the Data Lakehouse:
-	Closed records should be treated as Archives, that have unique natural/business PKs for the entire dataset.
-	Open records should be treated as Snapshots, that have unique natural/business PKs within each snapshot.
-	Tables with mixed records should be separated. When it’s not possible to do so, they should either be treated as Snapshots or an updatable Archive (again, foreachBatch idempotence!).
### Self-Service Analytics and Handling Asynchrony - The Semantic Layer
The final element of the architecture is a powerful query engine (Photon) which lets the data consumers create aggregations and integration efficiently for themselves. Here, ad-hoc queries can be written and executed, an analytics-specific schema (Snowflake, Star, etc.) can be applied, tables can be joined, unioned, grouped, etc. These operations are not necessarily re-calculated every time when a user executes a query as caching query results is supported by Databricks SQL.

This layer, together with the thin analytical applications (for example Power BI) depending on it, is called the Semantic Layer and it is absolutely necessary to preserve data quality and to provide flexibility for data consumers. The main reasons for this are:

- Aggregated data always contains less information than its' granular source, which restricts its' analytical potential.
- Physical aggregation and integration are extremely sensitive to asynchronous data arrival, which makes them impossible to use with streaming and late arriving data without losing a (possibly large) portion of the data.

An important element of this layer is UDF support, as some of the complex transformations are impractical or impossible to express through SQL. As of yet, this feature isn't included in Databricks SQL, however, there's a workaround that involves developing (and unit testing) the UDFs in separate Scala projects, compiling them to JAR, and registering them to Hive as permanent functions:<br>
[https://spark.apache.org/docs/3.3.0/sql-ref-syntax-ddl-create-function.html](https://spark.apache.org/docs/3.3.0/sql-ref-syntax-ddl-create-function.html)<br>
[https://docs.databricks.com/sql/language-manual/sql-ref-functions-udf-hive.html](https://docs.databricks.com/sql/language-manual/sql-ref-functions-udf-hive.html)

These dynamic Databricks SQL "databases" should have a single datamart as a source, to minimize the number of dependencies, thus the risk of unintentional breaking. As a consequence of this strategy, each dynamic "database" will reflect a single gold (and silver) zone, which means these "databases" can be called Platinum modules and treated as the final layers of datamarts.
## Sources
**Data Mesh**
[https://www.datamesh-architecture.com/](https://www.datamesh-architecture.com/)<br>
**Medallion Architecture**
[https://www.databricks.com/glossary/medallion-architecture](https://www.databricks.com/glossary/medallion-architecture)<br>
[https://www.youtube.com/watch?v=LJtShrQqYZY&t=2489s](https://www.youtube.com/watch?v=LJtShrQqYZY&t=2489s)<br>
**Semantic Lakehouse**
[https://www.databricks.com/kr/dataaisummit/session/how-implement-semantic-layer-your-lakehouse](https://www.databricks.com/kr/dataaisummit/session/how-implement-semantic-layer-your-lakehouse)<br>
[https://www.youtube.com/watch?v=p3TLEV3oIBY](https://www.youtube.com/watch?v=p3TLEV3oIBY)<br>
[https://assets.ctfassets.net/qy0rf2gaydgl/4zPJPPqu5OUkYeQw8MSWo0/c8e9d55489f47013733c8e10a30659d9/AtScale_TechnicalOverview_July_2019.pdf](https://assets.ctfassets.net/qy0rf2gaydgl/4zPJPPqu5OUkYeQw8MSWo0/c8e9d55489f47013733c8e10a30659d9/AtScale_TechnicalOverview_July_2019.pdf)<br>
[https://medium.com/@kyle.hale/querying-one-trillion-rows-of-data-with-powerbi-and-azure-databricks-c7e64ae9abda](https://medium.com/@kyle.hale/querying-one-trillion-rows-of-data-with-powerbi-and-azure-databricks-c7e64ae9abda)<br>
[https://medium.com/@kyle.hale/architecting-aggregations-in-powerbi-with-databricks-sql-675899014ce3](https://medium.com/@kyle.hale/architecting-aggregations-in-powerbi-with-databricks-sql-675899014ce3)<br>
[https://techcommunity.microsoft.com/t5/analytics-on-azure-blog/the-semantic-lakehouse-with-azure-databricks-and-power-bi/ba-p/3255174](https://techcommunity.microsoft.com/t5/analytics-on-azure-blog/the-semantic-lakehouse-with-azure-databricks-and-power-bi/ba-p/3255174)<br>
[https://learn.microsoft.com/en-us/azure/architecture/solution-ideas/articles/azure-databricks-modern-analytics-architecture](https://learn.microsoft.com/en-us/azure/architecture/solution-ideas/articles/azure-databricks-modern-analytics-architecture)<br>
**Functional Data Engineering**
[https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a](https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a)<br>
[https://www.youtube.com/watch?v=4Spo2QRTz1k&t=1808s](https://www.youtube.com/watch?v=4Spo2QRTz1k&t=1808s)<br>