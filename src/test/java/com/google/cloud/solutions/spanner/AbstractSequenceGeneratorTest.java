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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

class AbstractSequenceGeneratorTest {

  static final String SEQUENCE_NAME = "my_sequence";
  @Mock public DatabaseClient dbClient;
  @Mock public TransactionRunner txRunner;
  @Mock public TransactionContext txContext;
  @Captor public ArgumentCaptor<Mutation> mutationCapture;

  static Mutation buildUpdateNextValueMutation(long newValue) {
    return Mutation.newUpdateBuilder(SEQUENCES_TABLE)
        .set(SEQUENCE_NAME_COLUMN)
        .to(SEQUENCE_NAME)
        .set(NEXT_VALUE_COLUMN)
        .to(newValue)
        .build();
  }

  @Before
  public void setUp() {
    when(dbClient.readWriteTransaction()).thenReturn(txRunner);
    when(txRunner.run(any()))
        .then(
            (invocation) -> {
              TransactionCallable transactionCallable = invocation.getArgument(0);
              return transactionCallable.run(txContext);
            });
    doNothing().when(txContext).buffer(mutationCapture.capture());
  }
}
