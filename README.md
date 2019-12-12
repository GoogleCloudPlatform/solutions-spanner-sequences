# Spanner Sequence Generator example code

This repository contains example code to accompany the
[Sequence generation in Cloud Spanner](https://cloud.google.com/solutions/sequence-generation-in-cloud-spanner)
solution paper.

## How to use this example

The example code includes performance tests of the different types of sequence
generators. These tests require a Cloud Spanner database.

1.  Create a Cloud Spanner instance (if required). One node is sufficient for
    these tests.

1.  Create a Cloud Spanner database with the following table definition:

    ```sql
    CREATE TABLE sequences (
      name STRING(64) NOT NULL,
      next_value INT64 NOT NULL,
    ) PRIMARY KEY(name)
    ```

1.  Insert the following single row into the database:

    ```sql
    INSERT INTO sequences (name, next_value) VALUES ('my-sequence', 0);
    ```

1.  Clone the repository from GitHub

    ```sh
    git clone https://github.com/GoogleCloudPlatform/solutions-spanner-sequences.git
    ```

1.  If running on a Compute Engine VM, ensure that the service account being
    used has access to the Cloud Spanner API and can read/write to the Cloud
    Spanner Database. \
    If running on a development machine ensure that your Application Default
    Credentials are set to an account that can read/write to the Cloud Spanner
    database. \
    See [Authentication Overview](https://cloud.google.com/docs/authentication/)
    in Google Cloud documentation for more information.

1.  Compile and run the performance test

    Either: compile and execute using Maven:

    ```sh
    mvn compile exec:java \
        -Dexec.mainClass=com.google.cloud.solutions.spanner.PerformanceTest \
        -Dexec.args="INSTANCE_NAME DATABASE_NAME MODE ITERATIONS THREADS"
    ```

    Or: build a 'fat' deployable JAR containing all dependencies (for easy
    copying to a remote machine), and execute the JAR:

    ```sh
    mvn package
    java -jar target/sequence-generator-1.0-SNAPSHOT-jar-with-dependencies.jar \
        INSTANCE_NAME DATABASE_NAME MODE ITERATIONS THREADS
    ```

    where:

    *   INSTANCE\_NAME is your Cloud Spanner instance name
    *   DATABASE\_NAME is the name of your database
    *   MODE is one of the sequence generator modes: _simple_, _sync_, _async_,
        _batch_ or _asyncbatch_. (see the solution document for the definition of these modes)
    *   ITERATIONS - the number of sequence values to generate
    *   THREADS - the number of simultaneous threads requesting sequence values

## License

[Apache Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
