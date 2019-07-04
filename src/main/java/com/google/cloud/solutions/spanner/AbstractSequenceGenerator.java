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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Base class for all sequence generators that use simple {@code getNext()} */
public abstract class AbstractSequenceGenerator {

  static final String SEQUENCES_TABLE = "sequences";
  static final String NEXT_VALUE_COLUMN = "next_value";
  static final String SEQUENCE_NAME_COLUMN = "name";
  protected final String sequenceName;
  private static final ExecutorService executor = Executors.newCachedThreadPool();

  public AbstractSequenceGenerator(String sequenceName) {
    this.sequenceName = sequenceName;
  }

  /**
   * Gets the next value from the sequence.
   */
  public abstract long getNext();

  /**
   * Gets the next value using a background thread - to be used when inside a transaction to avoid
   * Nested Transaction errors.
   */
  public long getNextInBackground() throws Exception {
    return executor.submit(this::getNext).get();
  }
}
