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
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import javax.annotation.Nullable;

/** Example usages of the various sequence generators. */
public final class UsageExamples {

  private UsageExamples() {}

  private DatabaseClient dbClient;

  // [START simpleUsage]
  // Simple Sequence generator created outside transaction, eg as field.
  private SimpleSequenceGenerator simpleSequence = new SimpleSequenceGenerator("my Sequence");

  public void usingSimpleSequenceGenerator() {
    dbClient
        .readWriteTransaction()
        .run(
            new TransactionCallable<Void>() {
              @Nullable
              @Override
              public Void run(TransactionContext txn) {
                // Get a sequence value
                long nextValue = simpleSequence.getNext(txn);
                // Use nextValue in the transaction
                // ...
                return null;
              }
            });
  }
  // [END simpleUsage]

  // [START syncUsage]
  public void usingSynchronousSequenceGenerator() {
    dbClient
        .readWriteTransaction()
        .run(
            new TransactionCallable<Void>() {
              @Nullable
              @Override
              public Void run(TransactionContext txn) {
                // Create the sequence generator object within the transaction
                SynchronousSequenceGenerator syncSequence =
                    new SynchronousSequenceGenerator("my_sequence", txn);
                // Get two sequence values
                long key1 = syncSequence.getNext();
                long key2 = syncSequence.getNext();
                // Use the 2 key values in the transaction
                // ...
                return null;
              }
            });
  }
  // [END syncUsage]

  // [START asyncUsage]
  // Async Sequence generator created outside transaction as a long-lived object.
  private AsynchronousSequenceGenerator myAsyncSequence =
      new AsynchronousSequenceGenerator("my Sequence", dbClient);

  public void usingAsynchronousSequenceGenerator() {
    // Get two sequence values
    final long key1 = myAsyncSequence.getNext();
    final long key2 = myAsyncSequence.getNext();
    dbClient
        .readWriteTransaction()
        .run(
            new TransactionCallable<Void>() {
              @Nullable
              @Override
              public Void run(TransactionContext txn) {
                // Use the 2 key values in the transaction
                // ...
                return null;
              }
            });
  }
  // [END asyncUsage]

  // [START batchUsage]
  // Batch Sequence generator created outside transaction, as a long-lived object.
  private BatchSequenceGenerator myBatchSequence =
      new BatchSequenceGenerator("my Sequence", /* batchSize= */ 100, dbClient);

  public void usingBatchSequenceGenerator() {
    // Get two sequence values
    final long key1 = myBatchSequence.getNext();
    final long key2 = myBatchSequence.getNext();
    dbClient
        .readWriteTransaction()
        .run(
            new TransactionCallable<Void>() {
              @Nullable
              @Override
              public Void run(TransactionContext txn) {
                // Use the 2 key values in the transaction
                // ...
                return null;
              }
            });
  }
  // [END batchUsage]

  // [START asyncBatchUsage]
  // Async Batch Sequence generator created outside transaction, as a long-lived object.
  private AsyncBatchSequenceGenerator myAsyncBatchSequence =
      new AsyncBatchSequenceGenerator("my Sequence", /* batchSize= */ 1000, 200, dbClient);

  public void usingAsyncBatchSequenceGenerator() {
    dbClient
        .readWriteTransaction()
        .run(
            new TransactionCallable<Void>() {
              @Nullable
              @Override
              public Void run(TransactionContext txn) {
                // Get two sequence values
                final long key1 = myBatchSequence.getNext();
                final long key2 = myBatchSequence.getNext();
                // Use the 2 key values in the transaction
                // ...
                return null;
              }
            });
  }
  // [END asyncBatchUsage]

}
