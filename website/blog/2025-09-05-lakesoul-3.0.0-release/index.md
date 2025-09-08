# LakeSoul 3.0.0 Released

After nearly a year of iterative optimization, LakeSoul 3.0.0 is officially released. This release brings the following key updates:

1. Core Functionality Updates for the LakeSoul Lake Warehouse Framework
    1. LakeSoul NativeIO performance has been significantly optimized, including adjustments to write file compression and dictionary encoding     algorithms, and optimizations to key Merge on Read code paths, resulting in a doubling of both read and write performance(compared to 2.6 version).
    2. LakeSoul NativeIO has added a local hot data caching feature. This allows remote object storage files to be cached on local disk,     significantly improving the performance of MPP queries and other queries. Local caching is supported for all types of remote storage.
    3. LakeSoul query partition filter pushdown performance has been significantly optimized. By using metadata index queries, pushdown of     equal-value partition filter conditions has been significantly optimized. In actual tests, partition filtering on a single table with     millions of partitions took only 50ms.
    4. Flink upgraded to version 1.20.
    5. LakeSoul natively supports the Spark + Gluten vectorization engine, significantly improving batch computing performance.
    6. LakeSoul natively supports the Presto + Velox vectorization engine, providing high-performance MPP on-lake analytics and queries. The     Presto engine has added RBAC permissions.
    7. Arrow Flight SQL RPC Service: Provides a high-performance columnar data read and write gateway service based on the Arrow Flight protocol, supporting load balancing, elastic scaling, and RBAC permission verification.
    8. Python packages are now available on PyPi, and the LakeSoul Python package can be directly installed via `pip install lakesoul`.
2. LakeSoul Lake Warehouse Backend Service
    1. A new generation of size-tiered automatic background compaction service, significantly improving compaction performance and significantly reducing write amplification, thereby lowering compaction resource overhead.
    2. A new generation of automatic asynchronous cleanup service: Asynchronously cleans redundant and expired data by consuming metadata change logs.
    3. Asset Statistics Service: Automatically generates lake warehouse asset statistics by consuming metadata change logs, providing real-time statistics on storage resource consumption across multiple dimensions, including space, namespace, table, partition, and user.