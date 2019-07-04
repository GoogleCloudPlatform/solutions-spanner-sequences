/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.solutions.spanner;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Stopwatch;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs performance tests on the various sequence generators
 *
 * <p>Needs a Spanner database with the following table, and a row containing: <br>
 * {@code ('my-sequence', 0)}
 *
 * <pre>
 *   CREATE TABLE sequences (
 * 	  name STRING(64) NOT NULL,
 * 	  next_value INT64 NOT NULL,
 *   ) PRIMARY KEY (name)
 *
 *   INSERT INTO sequences (name,next_value) VALUES ('my-sequence', 0);
 * </pre>
 *
 * Usage: {@code PerformanceTest instance database TYPE iterations threads [txnDelay ms]}
 *
 * <p>Where:
 *
 * <ul>
 *   <li>TYPE is one of [naive, sync, async, batch]
 *   <li>iterations is the number of sequence values to get
 *   <li>threads is the number of threads to use.
 *   <li>txnDelay is a delay in the transaction - to simulate the read/update latency of an actual
 *       transaction -- default 10ms
 * </ul>
 *
 * System
 */
public class PerformanceTest {
  private static final String SEQUENCE_NAME = "my-sequence";
  private final int numIterations;
  private final DatabaseClient dbClient;
  private final long transactionDelayMillis;
  private final ExecutorService executor;
  private final int numThreads;
  private final AtomicInteger counter = new AtomicInteger(0);
  private final long[] latencies;

  PerformanceTest(
      DatabaseClient dbClient, int numIterations, int numThreads, long transactionDelayMillis) {
    this.dbClient = dbClient;
    this.numIterations = numIterations;
    this.executor = Executors.newFixedThreadPool(numThreads);
    this.numThreads = numThreads;
    this.transactionDelayMillis = transactionDelayMillis;
    this.latencies = new long[numIterations];
  }

  private void runPerfTestNaive() {
    NaiveSequenceGenerator generator = new NaiveSequenceGenerator(SEQUENCE_NAME);
    runPerfTest(
        () ->
            dbClient
                .readWriteTransaction()
                .run(
                    txn -> {
                      long next_value = generator.getNext(txn);
                      // simulate transaction duration
                      Thread.sleep(transactionDelayMillis);
                      return null;
                    }));
  }

  private void runPerfTestSync() {
    runPerfTest(
        () ->
            dbClient
                .readWriteTransaction()
                .run(
                    txn -> {
                      SynchronousSequenceGenerator generator =
                          new SynchronousSequenceGenerator(SEQUENCE_NAME, txn);
                      long next_value = generator.getNext();
                      // simulate transaction duration
                      Thread.sleep(transactionDelayMillis);
                      return null;
                    }));
  }

  private void runPerfTestAsync() {
    AsynchronousSequenceGenerator generator =
        new AsynchronousSequenceGenerator(SEQUENCE_NAME, dbClient);
    runPerfTest(
        () -> {
            long next_value = generator.getNext();
            dbClient
                .readWriteTransaction()
                .run(
                    txn -> {
                      // simulate transaction duration
                      Thread.sleep(transactionDelayMillis);
                      return null;
                    });

        });
  }
  private void runPerfTestBatch() {
    BatchSequenceGenerator generator = new BatchSequenceGenerator(SEQUENCE_NAME, 50, dbClient);
    runPerfTest(
        () -> {
          long next_value = generator.getNext();
           dbClient
                .readWriteTransaction()
                .run(
                    txn -> {
                      // simulate transaction duration
                      Thread.sleep(transactionDelayMillis);
                      return null;
                    });
        });
  }

  private void runPerfTest(Runnable r) {
    Stopwatch timer = Stopwatch.createStarted();
    for (int i = 0; i < numIterations; i++) {
      final int iteration = i; // need a final.
      executor.submit(
          () -> {
            try {
              Stopwatch latency = Stopwatch.createStarted();
              r.run();
              latencies[iteration] = latency.elapsed(TimeUnit.MILLISECONDS);
              counter.incrementAndGet();
            } catch (Throwable e) {
              e.printStackTrace();
            }
          });
    }
    executor.shutdown();
    boolean stopped = false;
    do {
      try {
        System.out.printf(
            "Waiting for tasks to finish... done: %d at %f/s\n",
            counter.get(), (counter.get() * 1000.0) / timer.elapsed(TimeUnit.MILLISECONDS));

        stopped = executor.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        /* ignore */
      }
    } while (!stopped);
    timer.stop();

    // Simple latency percentile calculator - sort all latencies and print %iles!
    Arrays.sort(latencies);
    System.out.printf(
        "%d iterations (%d parallel threads) in %d milliseconds: %f values/s\n",
        numIterations,
        numThreads,
        timer.elapsed(TimeUnit.MILLISECONDS),
        (numIterations * 1000.0) / timer.elapsed(TimeUnit.MILLISECONDS));
    System.out.printf("Latency: 50%%ile %d ms\n", latencies[(int) (numIterations * 0.5)]);
    System.out.printf("Latency: 75%%ile %d ms\n", latencies[(int) (numIterations * 0.75)]);
    System.out.printf("Latency: 90%%ile %d ms\n", latencies[(int) (numIterations * 0.90)]);
    System.out.printf("Latency: 99%%ile %d ms\n", latencies[(int) (numIterations * 0.99)]);
  }

  // args: instance, database, iterations, threads, [txnDelay]
  public static void main(String[] args) {
    if (args.length < 5 || args.length > 6) {
      usage();
    }
    try {
      String instance = args[0];
      String database = args[1];
      String type = args[2];
      int iterations = Integer.parseInt(args[3]);
      int threads = Integer.parseInt(args[4]);
      int txnDelay = 10;
      if (args.length == 6) {
        txnDelay = Integer.parseInt(args[5]);
      }

      SpannerOptions options = SpannerOptions.newBuilder().build();
      System.out.printf("Connecting to Instance: %s, Database %s\n", instance, database);

      DatabaseClient dbClient =
          options
              .getService()
              .getDatabaseClient(DatabaseId.of(options.getProjectId(), instance, database));

      // Do a quick DB connection check
      dbClient
          .singleUse()
          .executeQuery(
              Statement.of(
                  "Select next_value from "
                      + AbstractSequenceGenerator.SEQUENCES_TABLE
                      + " where name ='"
                      + SEQUENCE_NAME
                      + "'"));

      System.out.printf(
          "Running test type %s; %d iterations using %d threads\n", type, iterations, threads);
      PerformanceTest test = new PerformanceTest(dbClient, iterations, threads, txnDelay);

      switch (type.toLowerCase()) {
        case "naive":
          test.runPerfTestNaive();
          break;
        case "sync":
          test.runPerfTestSync();
          break;
        case "async":
          test.runPerfTestAsync();
          break;
        case "batch":
          test.runPerfTestBatch();
          break;
        default:
          usage();
      }

    } catch (Throwable e) {
      e.printStackTrace();
      usage();
    }
    System.exit(0);
  }

  private static void usage() {
    System.out.println(
        "Usage: PerformanceTest instance database TYPE iterations threads [txnDelay ms]");
    System.out.println("Where TYPE is one of [naive, sync, async, batch]");
    System.exit(1);
  }
}
