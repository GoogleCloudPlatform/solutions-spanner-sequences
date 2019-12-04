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

/**
 * Generates sequence values using a batch-request mechanism.
 *
 * <p>On {@link #getNext()}, a sequence value is issued from an internally-managed batch of values.
 * If the internal batch is exhausted, then get a new batch from the database.
 */
public class BatchSequenceGenerator extends AbstractDatabaseSequenceGenerator {

  private final long batchSize;

  private long next_value = Long.MAX_VALUE;
  private long last_value_in_batch =
      Long.MIN_VALUE; // initialise to less than next_value to force new batch.

  /**
   * Creates a sequence generator
   */
  public BatchSequenceGenerator(String sequenceName, long batchSize, DatabaseClient dbClient)
      throws SpannerException {
    super(sequenceName, dbClient);
    this.batchSize = batchSize;
  }

  // [START getNext]
  /**
   * Gets a new batch of sequence values from the database.
   *
   * <p>Reads next_value, increments it by batch size, then writes the updated next_value back.
   */
  private synchronized void getBatch() throws SpannerException {
    if (next_value <= last_value_in_batch) {
      // already have some values left in the batch - maybe this has been refreshed by another
      // thread.
      return;
    }
    next_value = getAndIncrementNextValueInDB(batchSize);
    last_value_in_batch = next_value + batchSize - 1;
  }

  /**
   * Returns the next value from this sequence, getting a new batch of values if necessary.
   *
   * When getting a new batch, it creates a separate transaction, so this must be called
   * <strong>outside</strong> any other transactions. See {@link #getNextInBackground()} for an
   * alternative version that uses a background thread
   */

  public synchronized long getNext() throws SpannerException {
    if (next_value > last_value_in_batch) {
      getBatch();
    }
    long value = next_value;
    next_value++;
    return value;
  }
  // [END getNext]

  @Override
  public synchronized long getNextInBackground() throws Exception {
    // If the batch needs to be refreshed, then this uses a background thread.
    if (next_value > last_value_in_batch) {
      return super.getNextInBackground();
    } else {
      return getNext();
    }
  }
}
