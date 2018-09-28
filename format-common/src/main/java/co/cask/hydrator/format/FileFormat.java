/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.format;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * FileFormat supported by the file based sources/sinks.
 */
public enum FileFormat {
  AVRO,
  PARQUET,
  TEXT;

  /**
   * Functionally like the valueOf method except it throws a nicer error message about what the valid values are.
   */
  public static FileFormat from(String format) {
    try {
      return FileFormat.valueOf(format.toUpperCase());
    } catch (IllegalArgumentException e) {
      String values = Arrays.stream(FileFormat.values())
        .map(f -> f.name().toLowerCase())
        .collect(Collectors.joining(", "));
      throw new IllegalArgumentException(String.format("Invalid format '%s'. The value must be one of %s",
                                                       format, values));
    }
  }
}
