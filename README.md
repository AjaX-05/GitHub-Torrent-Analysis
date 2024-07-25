# GHTorrent Analytics (RDD)
This project analyzes data from the GHTorrent dataset using Apache Spark's RDD (Resilient Distributed Datasets) API. The analysis includes extracting and processing information about GitHub repositories, transactions, and failed HTTP requests.

# Overview
GHTorrent is an offline mirror of the data from the GitHub REST API, capturing most data related to pull requests on GitHub, excluding the actual code. This project uses Apache Spark to process and analyze this large-scale dataset.

# Key RDD Transformations
The project employs several essential RDD transformations for data processing:

Filter: Used to select specific lines or records based on conditions.
Map: Transforms each record in the RDD into another value or structure.
FlatMap: Similar to map, but can return multiple values for each input element.
ReduceByKey: Aggregates values for each key using a specified associative function.
SortBy: Orders the RDD based on a specified key.

# Optimization Techniques
To enhance performance, the following optimization techniques were applied:

Persistence: Storing intermediate RDDs in memory to reduce computation time for repeated actions.
Partitioning: Efficient data partitioning to balance the workload across the cluster.
Lazy Evaluation: Leveraging Spark's lazy evaluation to optimize the execution plan before processing the data.

# Contributing
Contributions are welcome! Please open an issue or submit a pull request for any suggestions or improvements.

