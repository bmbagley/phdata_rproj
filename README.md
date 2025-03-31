# Interview Round: R to PySpark Converstion

This repo provides a container for running the pyspark code in a container with docker and vscode with a jupyter notebook environment using a local PySpark 1-node.

## Current Progress
The R files provided are not fully refactored to Pyspark, but significant progress has been made to increase useability and readability.

## Responses to interview questions
- **How would you utilize Databricks clusters for efficient processing?**
    - Databricks clusters could be optimized based on the size of data expected to be ingested, using compute-optimized nodes based on the current data sizes, partitioning the data in the largest tables by year or month, and using spark's robust runtime monitoring to track multiple runs and identify inneficencies.
- **How would you take advantage of Databricks notebooks for collaboration and documentation?**
    - Notebooks can be leveraged throught the process of development and reporting within an organization.  Within databricks the notebooks allow data to be shared across users & groups, teams can use built-in and external libraries to develop new models or pipelines, and code can be developed, tested, and deployed using notebooks in Databricksd Pipelines when combined with a ci/cd process with version version-control.  
    Additionally, many ml developers are comfortable with notebooks when developing so that incremental progress can be tracked within the runtime enviornment and documentation can be built-in.
- **How would you incorporate Databricks Delta Lake for data storage and management?**
    - The Databricks Delta Lake would improve the current process dramatically through the incremental load and processing features, ability to catalog & share tables, and connect with ML managment platforms to include additional data governance in ML processes.  
    Using a delta lake would allow the CSV's to be ingested incrementally, partitioned by a date field for more efficent processing fan-out among resources, and shared with other users via a more robust catalog.
    Delta tables are the default when building data products (tables) using the Databricks delta lake, they enable 
- **How would you leverage Databricks jobs for scheduling and automation?**
    - The Databricks DLT is the pipeline framework that could be used to automate an even-driven and incremental ELT pipeline.  The raw files would be ingested to the Delta Lake then into Delta Tables, leveraging incremental ingestion of the raw tables.  The DLT framwork also allows for develolpment of event-driven aggregation providing the ability to trigger aggregation on new incoming data and reduce the amount of total resources to perform aggregations.  
    Lastly incorporating monitoring and testing (error handling) into all steps of the Pipeline orchestration would be leveraged to make the pipeline resiliant.  Adding unit tests to individual jobs or notebooks would increase data quality and decrease error response times.  Monitoring of jobs within the pipelines, like tracking job runtimes, is also used to identify issues quicker and build trust in the data platform.
- **What other Databricks features would you use to simplify the given code base?**
    - Processing of the current implementation is based on static files that are a maximum of 72mb containing years of data into a static output.  The tables could be built as incremental datasets

## Runtime Requirements
- Running locally in docker
    See jPlane's setup to run in a container from github at [pyspark-devcontainer](https://github.com/jplane/pyspark-devcontainer) page
- To run the pyspark code
    Activate the conda environment and run `src/ConvertMe.py` to start a spark session and return output
