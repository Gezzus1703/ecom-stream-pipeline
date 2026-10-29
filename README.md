\# ⚡ E-Commerce Streaming Data Pipeline



An end-to-end e-commerce streaming pipeline built with \*\*Azure Event Hubs, Databricks, Spark Structured Streaming, Delta Lake, ADLS Gen2 and Power BI\*\*.



The project simulates e-commerce orders, streams them through Azure Event Hubs, processes them using a \*\*Bronze → Silver → Gold\*\* architecture and exposes aggregated data through Databricks SQL Warehouse for business reporting.



\---



\## 🏗️ Architecture



```mermaid

flowchart LR

&#x20;   A\[Python Order Simulator] --> B\[Azure Event Hubs]

&#x20;   B --> C\[Databricks]

&#x20;   C --> D\[Bronze Delta]

&#x20;   D --> E\[Silver Delta]

&#x20;   E --> F\[Gold Delta]

&#x20;   F --> G\[Databricks SQL Warehouse]

&#x20;   G --> H\[Power BI]



&#x20;   D --> I\[ADLS Gen2]

&#x20;   E --> I

&#x20;   F --> I

```



\### End-to-End Flow



```mermaid

flowchart TD

&#x20;   A\[Generate Order] --> B\[Publish Event]

&#x20;   B --> C\[Azure Event Hubs]

&#x20;   C --> D\[Spark Structured Streaming]

&#x20;   D --> E\[Bronze]

&#x20;   E --> F\[Data Cleaning]

&#x20;   F --> G\[Silver]

&#x20;   G --> H\[Event-Time Aggregation]

&#x20;   H --> I\[Gold]

&#x20;   I --> J\[Databricks SQL Warehouse]

&#x20;   J --> K\[Power BI Dashboard]

```



\---



\# 📊 Final Dashboard



The Gold layer is exposed through Databricks SQL Warehouse and used as the analytical source for Power BI.



!\[Power BI Dashboard](powerbi/dashboard-preview.png)



The dashboard provides an analytical view of the generated e-commerce data, including sales, items, states, categories and delivery status.



\---



\# 🔗 Databricks Job Lineage



The Databricks workflow connects the Bronze, Silver and Gold processing stages.



```mermaid

flowchart LR

&#x20;   A\[01 Stream Orders] --> B\[Bronze]

&#x20;   B --> C\[02 Clean Values]

&#x20;   C --> D\[Silver]

&#x20;   D --> E\[03 Aggregate to Gold]

&#x20;   E --> F\[Gold]

&#x20;   F --> G\[SQL Warehouse]

&#x20;   G --> H\[Power BI]

```



Example lineage from the completed Databricks job:



!\[Databricks Job Lineage](databricks\_notebooks/databricks\_job\_lineage.png)



\---



\# 🔄 Pipeline



\## 1. Order Generation



The pipeline starts with a Python simulator that generates synthetic e-commerce orders.



Each event contains:



| Field | Description |

|---|---|

| `order\_id` | Unique order identifier |

| `timestamp` | Event timestamp |

| `customer\_id` | Customer identifier |

| `product\_id` | Product identifier |

| `category` | Product category |

| `price` | Product price |

| `quantity` | Quantity ordered |

| `total\_amount` | Order value |

| `city` | Customer city |

| `state` | Customer state |

| `country` | Customer country |

| `latitude` | Customer latitude |

| `longitude` | Customer longitude |

| `delivery\_status` | Processing / Shipped / Delivered / Cancelled |



The simulator publishes the events to Azure Event Hubs using its Kafka-compatible endpoint.



\---



\# 2. Azure Event Hubs



Azure Event Hubs is used as the event ingestion layer between the Python producer and Databricks.



```mermaid

flowchart LR

&#x20;   A\[Python Producer] --> B\[Kafka Protocol]

&#x20;   B --> C\[Azure Event Hubs]

&#x20;   C --> D\[Databricks Spark]

```



Databricks consumes events from the `ecommerce-orders` Event Hub using Spark's Kafka source.



\---



\# 🥉 3. Bronze Layer



Notebook:



```text

databricks\_notebooks/01\_stream\_orders\_to\_bronze.py

```



The Bronze layer stores incoming events in Delta format with minimal transformation.



```mermaid

flowchart LR

&#x20;   A\[Azure Event Hubs] --> B\[Spark Structured Streaming]

&#x20;   B --> C\[Parse JSON]

&#x20;   C --> D\[Bronze Delta]

&#x20;   D --> E\[ADLS Gen2]

```



The incoming JSON payload is parsed using an explicit Spark schema before being written to the Bronze layer.



The Bronze layer acts as the first persistent stage of the pipeline.



\---



\# 🥈 4. Silver Layer



Notebook:



```text

databricks\_notebooks/02\_clean\_values\_silver.py

```



The Silver layer cleans and validates the Bronze data before analytical processing.



```mermaid

flowchart LR

&#x20;   A\[Bronze Delta] --> B\[Convert Timestamp]

&#x20;   B --> C\[Handle Null Values]

&#x20;   C --> D\[Recalculate Total]

&#x20;   D --> E\[Remove Duplicates]

&#x20;   E --> F\[Filter Invalid Records]

&#x20;   F --> G\[Silver Delta]

```



Main transformations:



\- Convert `timestamp` from string to Spark timestamp

\- Replace missing `price` values

\- Replace missing `quantity` values

\- Recalculate `total\_amount`

\- Remove duplicate `order\_id` values

\- Keep only records where `country = USA`

\- Remove records without a `state`



The result is a cleaner dataset ready for aggregation.



\---



\# 🥇 5. Gold Layer



Notebook:



```text

databricks\_notebooks/03\_aggregated\_to\_gold.py

```



The Gold layer transforms individual orders into aggregated, analytics-ready data.



```mermaid

flowchart LR

&#x20;   A\[Silver Delta] --> B\[1 Minute Watermark]

&#x20;   B --> C\[1 Minute Event-Time Window]

&#x20;   C --> D\[Group by State]

&#x20;   D --> E\[Group by Category]

&#x20;   E --> F\[Group by Delivery Status]

&#x20;   F --> G\[Sum Sales]

&#x20;   G --> H\[Sum Items]

&#x20;   H --> I\[Gold Delta]

```



\---



\## Event-Time Windows



The Gold aggregation uses the event's `timestamp` instead of simply grouping records by processing time.



The pipeline uses:



\- \*\*1-minute event-time windows\*\*

\- \*\*1-minute watermark\*\*

\- `state`

\- `category`

\- `delivery\_status`



```mermaid

flowchart LR

&#x20;   A\[Incoming Events] --> B\[Event Timestamp]

&#x20;   B --> C\[1 Minute Watermark]

&#x20;   C --> D\[1 Minute Event-Time Window]

&#x20;   D --> E\[State]

&#x20;   E --> F\[Category]

&#x20;   F --> G\[Delivery Status]

&#x20;   G --> H\[Aggregated Metrics]

```



\### Gold grain



Each Gold record represents:



\*\*1-minute event-time window × state × category × delivery status\*\*



The pipeline calculates:



\- `total\_sales` = sum of `total\_amount`

\- `total\_items` = sum of `quantity`



\### Gold schema



| Column | Description |

|---|---|

| `window\_start` | Start of the event-time window |

| `window\_end` | End of the event-time window |

| `state` | US state |

| `category` | Product category |

| `delivery\_status` | Delivery status |

| `total\_sales` | Total sales |

| `total\_items` | Total quantity sold |



Example:



```text

window\_start        window\_end          state  category       delivery\_status  total\_sales  total\_items

2026-10-28 13:22    2026-10-28 13:23    CA     Electronics    Delivered         2450.50      4

```



\---



\# ⏱️ Streaming Trigger



The notebooks use:



```python

.trigger(availableNow=True)

```



`availableNow=True` processes the data currently available and then terminates the query.



This means the project uses \*\*bounded streaming runs\*\* rather than an always-running streaming job.



New events can be processed by starting the pipeline again.



\---



\# 📈 6. Power BI



The Gold data is exposed through \*\*Databricks SQL Warehouse\*\* and consumed by Power BI.



```mermaid

flowchart LR

&#x20;   A\[Gold Delta] --> B\[Databricks SQL Warehouse]

&#x20;   B --> C\[Power BI]

&#x20;   C --> D\[Sales]

&#x20;   C --> E\[Items]

&#x20;   C --> F\[States]

&#x20;   C --> G\[Categories]

&#x20;   C --> H\[Delivery Status]

```



The final report can be used to analyse:



\- total sales

\- total items sold

\- sales by state

\- sales by category

\- delivery status

\- sales over time



\---



\# 🧰 Tech Stack



| Technology | Purpose |

|---|---|

| \*\*Python\*\* | Synthetic order generation |

| \*\*Azure Event Hubs\*\* | Event ingestion |

| \*\*Apache Spark\*\* | Stream processing |

| \*\*Databricks\*\* | Data engineering platform |

| \*\*Delta Lake\*\* | Data storage and processing |

| \*\*ADLS Gen2\*\* | Cloud storage |

| \*\*Databricks SQL Warehouse\*\* | SQL analytics |

| \*\*Power BI\*\* | Business intelligence |



\---



\# 📁 Project Structure



```text

ecom-stream-pipeline/

│

├── databricks\_notebooks/

│   ├── 01\_stream\_orders\_to\_bronze.py

│   ├── 02\_clean\_values\_silver.py

│   ├── 03\_aggregated\_to\_gold.py

│   └── databricks\_job\_lineage.png

│

├── powerbi/

│   └── dashboard-preview.png

│

├── simulator/

│   └── generate\_orders.py

│

└── README.md

```



\---



\# 🚀 Running the Pipeline



\### 1. Configure Azure Event Hubs



Create the Event Hub:



```text

ecommerce-orders

```



Configure the required Databricks credentials for accessing Event Hubs.



\### 2. Start the simulator



Run:



```text

simulator/generate\_orders.py

```



The simulator starts publishing synthetic orders.



\### 3. Run the Databricks pipeline



Execute the notebooks in order:



```text

01\_stream\_orders\_to\_bronze.py

&#x20;       ↓

02\_clean\_values\_silver.py

&#x20;       ↓

03\_aggregated\_to\_gold.py

```



\### 4. Connect Power BI



Connect Power BI to the Databricks SQL Warehouse and use the Gold dataset for reporting.



\---



\# 🎯 Key Concepts



This project demonstrates:



\- Event-driven data ingestion

\- Azure Event Hubs

\- Kafka-compatible ingestion

\- Spark Structured Streaming

\- Event-time processing

\- Watermarks

\- Window aggregations

\- Medallion Architecture

\- Delta Lake

\- ADLS Gen2

\- Databricks SQL Warehouse

\- Power BI



\---



\# 👨‍💻 Author



\*\*Michał Sitarz\*\*



Data / Analytics Engineering enthusiast focused on:



\*\*SQL • Python • Databricks • Azure • Spark • dbt • Power BI\*\*



\[GitHub](https://github.com/Gezzus1703)

