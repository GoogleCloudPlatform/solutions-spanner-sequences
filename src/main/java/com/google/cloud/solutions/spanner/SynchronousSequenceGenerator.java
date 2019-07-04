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

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import java.util.Collections;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

/** Sequence generator that can issue multiple valued from within the same transaction. */
public class SynchronousSequenceGenerator extends AbstractSequenceGenerator {

  // [START getNext]
  private final TransactionContext txn;
  @Nullable private Long nextValue;

  /** Creates a sequence generator for this transaction. */
  public SynchronousSequenceGenerator(String sequenceName, TransactionContext txn) {
    super(sequenceName);
    this.txn = txn;
  }

  /**
   * Returns the next value from this sequence.
   *
   * <p>Can be called multiple times in a transaction.
   */
  public long getNext() {
    if (nextValue == null) {
      // nextValue is unknown - read it.
      Struct result =
          txn.readRow(
              SEQUENCES_TABLE, Key.of(sequenceName), Collections.singletonList(NEXT_VALUE_COLUMN));
      if (result == null) {
        throw new NoSuchElementException(
            "Sequence " + sequenceName + " not found in table " + SEQUENCES_TABLE);
      }
      nextValue = result.getLong(0);
    }
    long value = nextValue;
    // increment and write nextValue to the database.
    nextValue++;
    txn.buffer(
        Mutation.newUpdateBuilder(SEQUENCES_TABLE)
            .set(SEQUENCE_NAME_COLUMN)
            .to(sequenceName)
            .set(NEXT_VALUE_COLUMN)
            .to(nextValue)
            .build());
    return value;
  }
  // [END getNext]
}
