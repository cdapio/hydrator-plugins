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

package co.cask.hydrator.plugin.db.batch;

import java.sql.Connection;
import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * Utility class for dealing with {@link Connection}'s transaction isolation level constants. This class
 * is needed primarily because the Connection class uses int constants, instead of Enums.
 */
public final class TransactionIsolationLevel {
  public static final String CONF_KEY = "co.cask.hydrator.db.plugin.transaction.isolation.level";

  private TransactionIsolationLevel() {
  }

  /**
   * Enum mapping to the constants in {@link Connection}.
   */
  public enum Level {
    TRANSACTION_NONE(0),
    TRANSACTION_READ_UNCOMMITTED(1),
    TRANSACTION_READ_COMMITTED(2),
    TRANSACTION_REPEATABLE_READ(4),
    TRANSACTION_SERIALIZABLE(8);

    private final int level;

    Level(int level) {
      this.level = level;
    }
  }


  /**
   * Translates a transaction isolation level string to the corresponding int constant.
   * For instance, if given "TRANSACTION_NONE", will return 0, as that is the
   * value of {@link Connection#TRANSACTION_NONE}. Defaults to {@link Connection#TRANSACTION_SERIALIZABLE}
   * if the given input is {@code null}.
   *
   * @param level String version of the level.
   * @return int corresponding to the constant in {@link Connection} for the level.
   */
  public static int getLevel(@Nullable String level) {
    if (level == null) {
      return Connection.TRANSACTION_SERIALIZABLE;
    }
    return Level.valueOf(level.toUpperCase()).level;
  }

  /**
   * Validates that the given level is either null or one of the possible transaction isolation levels.
   *
   * @param level the level to check
   */
  public static void validate(@Nullable String level) {
    if (level == null) {
      return;
    }
    try {
      Level.valueOf(level.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format(
        "Transaction isolation level must be one of the following values: %s, but got: %s.",
        Arrays.toString(Level.values()), level));
    }
  }
}
