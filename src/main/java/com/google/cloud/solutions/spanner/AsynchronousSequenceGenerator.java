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
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Struct;
import java.util.Collections;
import java.util.NoSuchElementException;

/**
 * Generates sequence values asynchronously to any current transactions
 *
 * <p>On {@link #getNext()} this class creates a new transaction to read and increment the sequence
 * value in the database
 */
public class AsynchronousSequenceGenerator extends AbstractSequenceGenerator {

  private final DatabaseClient dbClient;

  /** Construct with the given sequence name and database client. */
  public AsynchronousSequenceGenerator(String sequenceName, DatabaseClient dbClient) {
    super(sequenceName);
    this.dbClient = dbClient;
  }

  // [START getNext]

  /** Returns the next value from this sequence.
   *
   * Uses a separate transaction so must be used <strong>outside</strong>any other transactions.
   * See {@link #getNextInBackground()} for an alternative version that uses a background thread
   */
  public long getNext() throws SpannerException {
    return dbClient
        .readWriteTransaction()
        .run(
            txn -> {
              Struct result =
                  txn.readRow(
                      SEQUENCES_TABLE,
                      Key.of(sequenceName),
                      Collections.singletonList(NEXT_VALUE_COLUMN));
              if (result == null) {
                throw new NoSuchElementException(
                    "Sequence " + sequenceName + " not found in table " + SEQUENCES_TABLE);
              }
              long value = result.getLong(0);
              txn.buffer(
                  Mutation.newUpdateBuilder(SEQUENCES_TABLE)
                      .set(SEQUENCE_NAME_COLUMN)
                      .to(sequenceName)
                      .set(NEXT_VALUE_COLUMN)
                      .to(value + 1)
                      .build());
              return value;
            });
  }
  // [END getNext]

}
