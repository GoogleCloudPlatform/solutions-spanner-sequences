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

import static com.google.cloud.solutions.spanner.AbstractSequenceGenerator.NEXT_VALUE_COLUMN;
import static com.google.cloud.solutions.spanner.AbstractSequenceGenerator.SEQUENCES_TABLE;
import static com.google.cloud.solutions.spanner.AbstractSequenceGenerator.SEQUENCE_NAME_COLUMN;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import java.util.Collections;
import java.util.NoSuchElementException;

/** Sequence generator that reads and increments the sequence in a transaction. */
public class NaiveSequenceGenerator {

  private final String sequenceName;

  public NaiveSequenceGenerator(String sequenceName) {
    this.sequenceName = sequenceName;
  }

  // [START getNext]
  /**
   * Returns the next value from this sequence.
   *
   * <p>Should only be called once per transaction.
   */
  long getNext(TransactionContext txn) {
    Struct result =
        txn.readRow(
            SEQUENCES_TABLE, Key.of(sequenceName), Collections.singletonList(NEXT_VALUE_COLUMN));
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
  }
  // [END getNext]
}
