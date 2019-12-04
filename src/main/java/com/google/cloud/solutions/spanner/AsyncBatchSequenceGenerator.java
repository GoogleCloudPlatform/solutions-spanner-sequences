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
import com.google.cloud.spanner.SpannerException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Generates sequence values using a batch-request mechanism which is refilled asynchronously.
 *
 * <p>On {@link #getNext()}, a sequence value is issued from an internally-managed batch of values.
 * If the remaining values in the internal batch is less than the low-water-mark, then a new batch
 * is retrieved asynchronously so that the next batch of values will be already available when the
 * current batch is exhausted.
 */
public class AsyncBatchSequenceGenerator extends AbstractDatabaseSequenceGenerator {

  private final long batchSize;
  private final long lowWaterMarkForRefresh;

  private long nextValue = Long.MAX_VALUE;
  private long lastValueInBatch =
      Long.MIN_VALUE; // initialise to less than next_value to force new batch.

  // Contains the result of the background refresh task.
  @VisibleForTesting
  Future<Long> pendingNextBatchStart = null;

  /**
   * Creates a sequence generator
   */
  public AsyncBatchSequenceGenerator(String sequenceName, long batchSize,
      long lowWaterMarkForRefresh, DatabaseClient dbClient)
      throws SpannerException {
    super(sequenceName, dbClient);
    this.batchSize = batchSize;
    this.lowWaterMarkForRefresh = lowWaterMarkForRefresh;
    Preconditions.checkState(lowWaterMarkForRefresh < batchSize,
        "The low water mark for the batch size must be less than the size of the batch.");

    // Trigger first background refresh.
    pendingNextBatchStart = executor.submit(this::readNextBatchFromDB);
  }

  // [START getNext]
  /**
   * Gets a new batch of sequence values from the database.
   *
   * <p>Reads nextValue, increments it by batch size, then writes the updated nextValue back.
   * Stores the resulting value in  nextBatchStartValue, ready for when the existing pool of values
   * is exhausted.
   */
  private Long readNextBatchFromDB() {
    return getAndIncrementNextValueInDB(batchSize);
  }

  /**
   * Returns the next value from this sequence.
   *
   * If the number of remaining values is below the low watermark, this triggers a background
   * request for new batch of values if necessary. Once the current batch is exhausted, then a the
   * new batch is used.
   */
  public synchronized long getNext() throws SpannerException {
    // Check if a batch refresh is required and is not already running.
    if (nextValue >= (lastValueInBatch - lowWaterMarkForRefresh) && pendingNextBatchStart == null) {
      // Request a new batch in the background.
      pendingNextBatchStart = executor.submit(this::readNextBatchFromDB);
    }

    if (nextValue > lastValueInBatch) {
      // batch is exhausted, we should have received a new batch by now.
      try {
        // This will block if the transaction to get the next value has not completed.
        long nextBatchStart = pendingNextBatchStart.get();
        lastValueInBatch = nextBatchStart + batchSize - 1;
        nextValue = nextBatchStart;
      } catch (InterruptedException | ExecutionException e) {
        if (e.getCause() instanceof SpannerException) {
          throw (SpannerException) e.getCause();
        }
        throw new RuntimeException("Failed to retrieve new batch in background", e);
      } finally {
        pendingNextBatchStart = null;
      }
    }
    // return next value.
    long value = nextValue;
    nextValue++;
    return value;
  }
  // [END getNext]

  @Override
  public synchronized long getNextInBackground() throws Exception {
    // All database access is done in the background so no need for any special behavior.
    return getNext();
  }
}
