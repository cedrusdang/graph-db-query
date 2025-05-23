# Graph Database Project: Australian Road Fatality 2024

**Institution**: University of Western Australia  
**Unit**: Data Warehouse  
**Author**: Cedrus Dang  
**Date**: 22 May 2025  
**Version**: 1.0  
**Tech Stack**: Neo4j | Python | Pandas | Cypher  

📄 **[Download Full Technical Report (PDF)](./technical%20report/Technical_report_24190901.pdf)**

## Project Summary

This project implements a graph-based analytical pipeline using Neo4j to analyze Australian road crash fatalities in 2024. The model adopts a dimensional snowflake schema and includes:

- Property graph schema design
- Automated ETL pipeline in Python
- Relationship loading and querying with Cypher
- Full technical report with visual schema and GDS analytics

## How to Use

### 1. Setup

- Install Python 3.8+ and Neo4j Desktop or Server (v4.x or later)
- Install required packages:

```bash
pip install -r requirements.txt
```

- Place `ARDD_Fatalities_Dec_2024.csv` into the `ELT_input/` folder

### 2. Run ETL

```bash
python ETL.py
```

This generates dimension and relationship CSVs into `ELT_output/`.

### 3. Load to Neo4j

Copy all CSV files in `ELT_output/` into Neo4j's `import/` directory.

Use the Cypher commands from `Cypher.txt`, or examples below:

#### Load Node Example

```cypher
LOAD CSV WITH HEADERS FROM 'file:///date.csv' AS row
MERGE (:date {
  year: row.year,
  month: row.month,
  dayweek: row.dayweek,
  day_of_week: row.day_of_week,
  date_sk: row.date_sk
});
```

#### Load Relationship Example

```cypher
LOAD CSV WITH HEADERS FROM 'file:///IN_CRASH.csv' AS row
MATCH (a:fatality {fatality_sk: row.fatality_sk})
MATCH (b:crash {crash_sk: row.crash_sk})
MERGE (a)-[:IN_CRASH]->(b);
```

## Documentation

The technical report includes schema diagrams, design rationale, sample queries, and community detection results (GDS Louvain).

- Open `.Rmd` in RStudio or view the compiled `.pdf` version

## References

- Australian Road Deaths Database  
- Neo4j Documentation: Cypher, Graph Data Science  
- Blondel, V. D., Guillaume, J. L., Lambiotte, R., & Lefebvre, R. (2008). *Fast unfolding of communities in large networks*. [DOI](https://doi.org/10.1088/1742-5468/2008/10/P10008)
