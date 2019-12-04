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
import com.google.cloud.spanner.Struct;
import java.util.Collections;
import java.util.NoSuchElementException;

/**
 * Base class for all sequence generators that store and use a DatabaseClient
 */
public abstract class AbstractDatabaseSequenceGenerator extends AbstractSequenceGenerator {

  protected final DatabaseClient dbClient;

  public AbstractDatabaseSequenceGenerator(String sequenceName, DatabaseClient dbClient) {
    super(sequenceName);
    this.dbClient = dbClient;
  }

  // [START getAndIncrementNextValueInDB]
  /**
   * Gets the next sequence value from the database, and increments the database value by the amount
   * specified in a single transaction.
   */
  protected Long getAndIncrementNextValueInDB(long increment) {
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
                      .to(value + increment)
                      .build());
              return value;
            });
  }
  // [END getAndIncrementNextValueInDB]
}
