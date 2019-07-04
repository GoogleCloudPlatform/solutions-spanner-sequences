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
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class SynchronousSequenceGeneratorTest extends AbstractSequenceGeneratorTest {

  @Test
  public void getNext() {
    SynchronousSequenceGenerator generator =
        new SynchronousSequenceGenerator(SEQUENCE_NAME, txContext);

    // Setup next_value=22
    when(txContext.readRow(eq(SEQUENCES_TABLE), eq(Key.of(SEQUENCE_NAME)), anyList()))
        .thenReturn(Struct.newBuilder().set(NEXT_VALUE_COLUMN).to(22).build());

    assertThat(generator.getNext()).isEqualTo(22);
    // Verify updated next_value written back to disk.
    assertThat(mutationCapture.getValue())
        .isEqualTo(
            Mutation.newUpdateBuilder(SEQUENCES_TABLE)
                .set(SEQUENCE_NAME_COLUMN)
                .to(SEQUENCE_NAME)
                .set(NEXT_VALUE_COLUMN)
                .to(23L)
                .build());
  }

  @Test
  public void getNextTwice() {
    SynchronousSequenceGenerator generator =
        new SynchronousSequenceGenerator(SEQUENCE_NAME, txContext);

    // Setup next_value=22
    when(txContext.readRow(eq(SEQUENCES_TABLE), eq(Key.of(SEQUENCE_NAME)), anyList()))
        .thenReturn(Struct.newBuilder().set(NEXT_VALUE_COLUMN).to(22).build());

    assertThat(generator.getNext()).isEqualTo(22);
    assertThat(generator.getNext()).isEqualTo(23);

    // Verify updated next_values written back to disk.
    assertThat(mutationCapture.getAllValues().size()).isEqualTo(2);

    assertThat(mutationCapture.getAllValues().get(0))
        .isEqualTo(
            Mutation.newUpdateBuilder(SEQUENCES_TABLE)
                .set(SEQUENCE_NAME_COLUMN)
                .to(SEQUENCE_NAME)
                .set(NEXT_VALUE_COLUMN)
                .to(23L)
                .build());
    assertThat(mutationCapture.getAllValues().get(1))
        .isEqualTo(
            Mutation.newUpdateBuilder(SEQUENCES_TABLE)
                .set(SEQUENCE_NAME_COLUMN)
                .to(SEQUENCE_NAME)
                .set(NEXT_VALUE_COLUMN)
                .to(24L)
                .build());
  }
}
