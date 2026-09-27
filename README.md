# Databricks Demo

## Project Summary

A learning project that demonstrates AWS Databricks functionality.

## Databricks Ideas and Concepts

### General Concepts we Need to Understand

Some concepts I need to work through.

- Account
- Workspace

We need to know how these relate to these concepts.

- Metastore
- Catalog
- Schema (a.k.a. database)

### Data Storage and Access

Databricks is running on an AWS account inside its own VPC. Data can be hosted within that VPC or can be hosted elsewhere in the AWS account.

- Managed tables and volumes are fully managed by Databricks. Managed tables always use the Delta Lake format. It's unclear whether Databricks fully controls access to this data from within the AWS account.
- External tables/storage are external to Databricks, but Databricks has access. You can control access to this data from within Databricks, but other services in the AWS account can also access this data.

### Metastore

There is the Unity Catalog service manages metadata and permissions. It's a data governance solution. A Unity Catalog Metastore stores the metadata. Multiple workspaces are associated with a single Unity Catalog Metastore, and each metastore is associated with an AWS region.

### Architecture

![Databricks Architecture](https://docs.databricks.com/aws/en/assets/images/architecture-c2c83d23e2f7870f30137e97aaadea0b.png)

According to [the documentation](https://docs.databricks.com/aws/en/getting-started/overview#serverless-compute-plane) the serverless compute plane is not inside your Databricks VPC. It is centrally managed on Databricks AWS Account, with guardrails around data access and scaling to ensure there is no cross tenant contamination.

## Notes on Vanilla Spark

### Basic Architecture

According to the [architecture documentation](https://spark.apache.org/docs/latest/cluster-overview.html) "Spark applications run as independent processes on a cluster". Though many mentions in the documentation are made of access to a file system, unlike an RDMS, Spark does not persist data to disk unless asked to.

### Driver Program

You submit an application to the cluster of *worker nodes* as described [here](https://spark.apache.org/docs/latest/submitting-applications.html). Somewhere in your application you will need to instantiate a `SparkContext` object, which connects to a *cluster manager*. The cluster manager distributes the workload across the cluster. The part of the application with access to the `SparkContext` is called the *driver program* of the application.

Each node provides *executors* which are effectively just processes, i.e., which provide access to CPU and private memory. When a driver program starts, it acquires these executors and sends them JAR or python files. Finally, the `SparkContext` (not the cluster manager) sends tasks to the executors to run.

![cluster manager](https://spark.apache.org/docs/latest/img/cluster-overview.png)

Note, that because tasks run in their own processes and use private memory, you cannot share data between programs unless you write to an external file system.

Also note that the driver program needs network access to the worker nodes, so it's considered best practice to run the driver on the same local network as the worker nodes. If that's not possible then some sort of RPC setup is required. A driver program will also expose a monitoring web UI on port 4040 by default.

### Resilient Distributed Dataset

The underlying data structure in a Spark program is the resilient distributed dataset (RDD). An RDD is a multiset representing arbitrary data, and is an in memory object. As such, resilience needs to be achieved without persisting to disk. How can this be done?

The key here, is that every Spark program creates an execution plan following a functional paradigm. This means that each RDD is immutable, and each step in the execution plan produces a new RDD.

If a node in the cluster goes down, then the current RDD is lost because there is no backup on a persistent storage device. However, Spark will re-execute any upstream steps in the execution plan on a different node in order to recreate the RDD. Because all steps in the plan are functional, repeating the calculation will give identical results.

According to the [documentation](https://spark.apache.org/docs/latest/rdd-programming-guide.html), in vanilla Spark

```text
RDDs are created by starting with a file in the Hadoop file system (or any other Hadoop-supported file system), or an existing Scala collection in the driver program, and transforming it. Users may also ask Spark to persist an RDD in memory, allowing it to be reused efficiently across parallel operations.
```

Note, that "persist" in the above quote doesn't mean writing to persistent storage.

### Submitting Applications

The lowest level entrypoint for launching a Spark application is a binary executable `./bin/spark-submit` that accepts command line arguments. A minimal example from the [documentation](https://spark.apache.org/docs/latest/submitting-applications.html)

```text
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
```

The meanings are as follows:

- `--class`: The entrypoint of the application.
- `--master`: The URL of the cluster manager. It can take [many different formats](https://spark.apache.org/docs/latest/submitting-applications.html) depending on the cluster manager.
- `--deploy-mode`: Whether to start the driver program on the cluster or locally.
- `--conf`: Any Spark configuration as "key=value" pairs.
- `<application_jar>`: Your application's jar file.
- `[application_arguments]`: Your application's cli arguments.

For python you replace `<application-jar>` with a `.py` file. 

Dependencies can be added by indicating the location of files, i.e., `--jar-files` and `--py-files`. Those files can refer to URLs with various prefixes, e.g., `local` for local files, `hdfs` for files on a hdfs file store. Note that files are downloaded to a persistent volume on the each worker node, and these volumes need to be cleaned up from time to time.

In the case of python, you can include additional `.py` files directly in the spark submit command, or artifacts produced by [PEX](https://github.com/pex-tool/pex) or [venv-pack](https://jcristharif.com/venv-pack/index.html).

You do not need to include Spark itself as an application dependency. Each worker node automatically installs and injects this into  running tasks.

### MapReduce

[Wikipedia article](https://en.wikipedia.org/wiki/MapReduce) is really good.

- Data is split across several nodes, computation on each node produces key-value pairs.
- Those key-value pairs are shuffled between the nodes depending on the key.
- Each node reduces the keys it's responsible for and sends its chunk of the results back to the main program.

For example, if we are counting instances of different words in large text file.

- The file is split into chunks, with a chunk sent to each node. The node then calculates the number of instances of each word on the chunk it is responsible for. The keys are the unique words in its chunk, and the value is the number of instances.
- All key-value pairs with the same key, e.g., the word "horse", are sent to the same node.
- Each node then sums the instances of each word and sends its results back to the driver program.

Beyond MapReduce, Spark offers the ability to

- Filter results
- Iterate over the same piece of data without invoking separate MapReduce operations.

You can see [Computerphile video](https://www.youtube.com/watch?v=tDVPcqGpEnM)) or the [Databricks documentation on MapReduce](https://www.databricks.com/glossary/mapreduce) for more information.

### Shared Variables

- Broadcast: Cached in memory on all nodes.
- Accumulators: Variables that are added to like counters or sums.

## Running Spark Sandbox in a Shell

```shell
docker compose up -d && docker compose exec -it spark-sandbox sh
```

Then in the interactive shell, navigate `/opt/spark` and verify that that "bin/" directory and "README.md" file are present.

From here, try run the pyspark interactive shell by running `bin/pyspark`.

Inside this pyspark shell session, you can work through the instructions in the [Python Spark Basics](https://spark.apache.org/docs/latest/quick-start.html#basics) section of the Spark programming guide.

## Open Questions

### Databricks

- What is the Delta Lake storage layer?
- There seems to be many ways to provision the compute based on whether you're running a notebook or a SQL query.
- Databricks is built on top of Spark, and there's something about lazy vs. eager execution that needs to be understood.
- Does Databricks come with some sort of web based IDE? How does version tracking work in this case?
- What is [Databricks Connect](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python), and how does using it differ from using the Databricks IDE?

### Vanilla Spark

- Get a better understanding of [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- Get a better understanding of what's happening when you run Spark in "local" mode.
- How to program with an RDD as opposed to a Dataset/DataFrame (link to [programming guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html))?
- What exactly is HDFS and how is it different from RDD? You can read the [Wikipedia article](https://en.wikipedia.org/wiki/Apache_Hadoop) or the Databricks explanation of it [here](https://www.databricks.com/glossary/hadoop-distributed-file-system-hdfs)
- Reread the [Spark glossary](https://spark.apache.org/docs/latest/job-scheduling.html).
- Read more about [python package management on Spark](https://spark.apache.org/docs/latest/api/python/user_guide/python_packaging.html).
- Understand Spark shuffling in this [article](https://medium.com/towards-data-architecture/spark-shuffling-395468fbf623) and [this article](https://medium.com/@philipp.brunenberg/understanding-apache-spark-shuffle-85644d90c8c6).

## Ideas for this Demo

### Vanilla Spark Ideas

- DONE: Locally write code in the PySpark shell, and run the PySpark shell locally following the [quickstart guide](https://spark.apache.org/docs/latest/quick-start.html).
- Run Spark in Docker, and connect to it from a python app using the [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) client.

### Databricks Ideas

- DONE: Set up a Databricks deployment in a single AWS region.
- Create a single Databricks workspace.
- Create a mix of managed and external data stores.
- Create some Spark notebooks or other Databricks assets.
- OPTIONAL: Create Unity Catalog metastore scoped to that region.
- OPTIONAL: Create a second workspace and associate it with the same metastore.