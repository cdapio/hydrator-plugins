/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.common;

import java.util.regex.Pattern;

/**
 * Utility class for Id related operations.
 */
public final class IdUtils {

  private IdUtils() {
  }

  private static final Pattern datasetIdPattern = Pattern.compile("[$\\.a-zA-Z0-9_-]+");

  public static void validateId(String id) throws IllegalArgumentException {
    if (!datasetIdPattern.matcher(id).matches()) {
      throw new IllegalArgumentException(String.format("%s is not a valid id. Allowed characters are letters, " +
                                                         "numbers, and _, -, ., or $.", id));
    }
  }
}
