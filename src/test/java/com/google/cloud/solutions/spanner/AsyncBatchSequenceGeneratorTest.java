package com.google.cloud.solutions.spanner;/*
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

import static com.google.cloud.solutions.spanner.AbstractSequenceGenerator.NEXT_VALUE_COLUMN;
import static com.google.cloud.solutions.spanner.AbstractSequenceGenerator.SEQUENCES_TABLE;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Struct;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AsyncBatchSequenceGeneratorTest extends AbstractSequenceGeneratorTest {

  @Test
  public void getNext() {
    // Setup next_value=201, batch size 10
    when(txContext.readRow(eq(SEQUENCES_TABLE), eq(Key.of(SEQUENCE_NAME)), anyList()))
        .thenReturn(Struct.newBuilder().set(NEXT_VALUE_COLUMN).to(201).build());

    AsyncBatchSequenceGenerator generator = new AsyncBatchSequenceGenerator(SEQUENCE_NAME, 10, 2,
        dbClient);

    assertThat(generator.getNext()).isEqualTo(201);

    // Verify that batch-incremented next_value written back to disk.
    verify(dbClient, times(1)).readWriteTransaction();
    assertThat(mutationCapture.getAllValues().size()).isEqualTo(1);
    assertThat(mutationCapture.getValue()).isEqualTo(buildUpdateNextValueMutation(211));
  }

  @Test
  public void getNextTwice() {
    // Setup next_value=201, batch size 10
    when(txContext.readRow(eq(SEQUENCES_TABLE), eq(Key.of(SEQUENCE_NAME)), anyList()))
        .thenReturn(Struct.newBuilder().set(NEXT_VALUE_COLUMN).to(201).build());

    AsyncBatchSequenceGenerator generator = new AsyncBatchSequenceGenerator(SEQUENCE_NAME, 10, 2,
        dbClient);

    assertThat(generator.getNext()).isEqualTo(201);
    assertThat(generator.getNext()).isEqualTo(202);

    // Verify that batch-incremented next_value written back to disk only once.
    verify(dbClient, times(1)).readWriteTransaction();
    assertThat(mutationCapture.getAllValues().size()).isEqualTo(1);
    assertThat(mutationCapture.getValue()).isEqualTo(buildUpdateNextValueMutation(211));
  }

  @Test
  public void getNextLowWaterMarkRefresh() throws ExecutionException, InterruptedException {
    // Setup next_value=201, then 211
    when(txContext.readRow(eq(SEQUENCES_TABLE), eq(Key.of(SEQUENCE_NAME)), anyList()))
        .thenReturn(Struct.newBuilder().set(NEXT_VALUE_COLUMN).to(201).build())
        .thenReturn(Struct.newBuilder().set(NEXT_VALUE_COLUMN).to(211).build());

    AsyncBatchSequenceGenerator generator = new AsyncBatchSequenceGenerator(SEQUENCE_NAME, 10, 5,
        dbClient);

    // Request 5 values...
    for (long i = 201; i < 206; i++) {
      assertThat(generator.getNext()).isEqualTo(i);
    }

    // wait for background refresh after 5 values
    assertThat(generator.pendingNextBatchStart.get()).isEqualTo(211);

    // Request another 9 values, should not trigger another BG refresh...
    for (long i = 206; i < 215; i++) {
      assertThat(generator.getNext()).isEqualTo(i);
    }

    // Check only 2 transactions were created
    verify(dbClient, times(2)).readWriteTransaction();

    // Verify updated next_value written back to DB twice, values 211, 221.
    assertThat(mutationCapture.getAllValues().size()).isEqualTo(2);

    assertThat(mutationCapture.getAllValues().get(0)).isEqualTo(buildUpdateNextValueMutation(211));
    assertThat(mutationCapture.getAllValues().get(1)).isEqualTo(buildUpdateNextValueMutation(221));
  }


  @Test
  public void getNextMany() throws ExecutionException, InterruptedException {
    // Setup next_value=201, then 211
    when(txContext.readRow(eq(SEQUENCES_TABLE), eq(Key.of(SEQUENCE_NAME)), anyList()))
        .thenReturn(Struct.newBuilder().set(NEXT_VALUE_COLUMN).to(201).build())
        .thenReturn(Struct.newBuilder().set(NEXT_VALUE_COLUMN).to(211).build())
        .thenReturn(Struct.newBuilder().set(NEXT_VALUE_COLUMN).to(221).build());

    AsyncBatchSequenceGenerator generator = new AsyncBatchSequenceGenerator(SEQUENCE_NAME, 10, 2,
        dbClient);

    // Request 20 values...
    for (long i = 201; i < 220; i++) {
      long val = generator.getNext();
      assertThat(val).isEqualTo(i);
    }

    assertThat(generator.pendingNextBatchStart.get()).isEqualTo(221);

    // Check only 3 transactions were created. (the third is for the unused next batch).
    verify(dbClient, times(3)).readWriteTransaction();

    // Verify updated next_value written back to DB 3 times, values 211, 221.
    assertThat(mutationCapture.getAllValues().size()).isEqualTo(3);

    assertThat(mutationCapture.getAllValues().get(0)).isEqualTo(buildUpdateNextValueMutation(211));
    assertThat(mutationCapture.getAllValues().get(1)).isEqualTo(buildUpdateNextValueMutation(221));
    assertThat(mutationCapture.getAllValues().get(2)).isEqualTo(buildUpdateNextValueMutation(231));
  }

}
