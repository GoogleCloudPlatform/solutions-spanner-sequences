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
import static com.google.common.truth.Truth.*;
import static org.mockito.Mockito.*;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

// Use Silent because not all Mocks from base class are used.
@RunWith(MockitoJUnitRunner.Silent.class)
public class NaiveSequenceGeneratorTest extends AbstractSequenceGeneratorTest {

  @Test
  public void getNext() {
    NaiveSequenceGenerator generator = new NaiveSequenceGenerator(SEQUENCE_NAME);

    // Setup next_value=22
    when(txContext.readRow(eq(SEQUENCES_TABLE), eq(Key.of(SEQUENCE_NAME)), anyList()))
        .thenReturn(Struct.newBuilder().set(NEXT_VALUE_COLUMN).to(22).build());

    assertThat(generator.getNext(txContext)).isEqualTo(22);
    // Verify updated next_value written back to disk.
    assertThat(mutationCapture.getValue()).isEqualTo(buildUpdateNextValueMutation(23));
  }

  @Test
  public void getNextTwice() {
    NaiveSequenceGenerator generator = new NaiveSequenceGenerator(SEQUENCE_NAME);

    // Setup next_value=22
    when(txContext.readRow(eq(SEQUENCES_TABLE), eq(Key.of(SEQUENCE_NAME)), anyList()))
        .thenReturn(Struct.newBuilder().set(NEXT_VALUE_COLUMN).to(22).build());

    assertThat(generator.getNext(txContext)).isEqualTo(22);
    assertThat(generator.getNext(txContext)).isEqualTo(22);

    // Verify same updated next_value written back to disk twice.
    assertThat(mutationCapture.getAllValues().get(0)).isEqualTo(buildUpdateNextValueMutation(23));
    assertThat(mutationCapture.getAllValues().get(1)).isEqualTo(buildUpdateNextValueMutation(23));
  }
}
