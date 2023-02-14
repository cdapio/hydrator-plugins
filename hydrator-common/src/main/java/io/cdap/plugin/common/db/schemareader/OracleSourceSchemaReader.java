/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.common.db.schemareader;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

/**
 * Oracle Source schema reader.
 */
public class OracleSourceSchemaReader extends CommonSchemaReader {
  /**
   * Oracle type constants, from Oracle JDBC Implementation.
   */
  public static final int INTERVAL_YM = -103;
  public static final int INTERVAL_DS = -104;
  public static final int TIMESTAMP_TZ = -101;
  public static final int TIMESTAMP_LTZ = -102;
  public static final int BINARY_FLOAT = 100;
  public static final int BINARY_DOUBLE = 101;
  public static final int BFILE = -13;
  public static final int LONG = -1;
  public static final int LONG_RAW = -4;

  /**
   * Logger instance for Oracle Schema reader.
   */
  private static final Logger LOG = LoggerFactory.getLogger(OracleSourceSchemaReader.class);

  public static final Set<Integer> ORACLE_TYPES = ImmutableSet.of(
    INTERVAL_DS,
    INTERVAL_YM,
    TIMESTAMP_TZ,
    TIMESTAMP_LTZ,
    BINARY_FLOAT,
    BINARY_DOUBLE,
    BFILE,
    LONG,
    LONG_RAW,
    Types.NUMERIC,
    Types.DECIMAL
  );

  private final String sessionID;

  public OracleSourceSchemaReader() {
    this(null);
  }

  public OracleSourceSchemaReader(String sessionID) {
    super();
    this.sessionID = sessionID;
  }

  @Override
  public Schema getSchema(String columnName, int sqlType, String sqlTypeName, String columnClassName,
                          int precision, int scale, boolean isSigned) throws SQLException {
    switch (sqlType) {
      case TIMESTAMP_TZ:
        return Schema.of(Schema.Type.STRING);
      case TIMESTAMP_LTZ:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case BINARY_FLOAT:
        return Schema.of(Schema.Type.FLOAT);
      case BINARY_DOUBLE:
        return Schema.of(Schema.Type.DOUBLE);
      case BFILE:
      case LONG_RAW:
        return Schema.of(Schema.Type.BYTES);
      case INTERVAL_DS:
      case INTERVAL_YM:
      case LONG:
        return Schema.of(Schema.Type.STRING);
      case Types.FLOAT:
        // When using the Connection.getMetadata().getColumns, Oracle Float type gets detected
        // as Types.FLOAT type, and should get handled similar to Double.
        return Schema.of(Schema.Type.DOUBLE);
      case Types.NUMERIC:
      case Types.DECIMAL:
        // FLOAT and REAL are returned as java.sql.Types.NUMERIC but with value that is a java.lang.Double
        if (Double.class.getTypeName().equals(columnClassName)) {
          return Schema.of(Schema.Type.DOUBLE);
        } else {
          // For a Number type without specified precision and scale, precision will be 0 and scale will be -127
          if (precision == 0) {
            // reference : https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1832
            LOG.warn(String.format("Field '%s' is a %s type without precision and scale, "
                    + "converting into STRING type to avoid any precision loss.",
                columnName,
                sqlType));
            return Schema.of(Schema.Type.STRING);
          }
          return Schema.decimalOf(precision, scale);
        }
      default:
        return super.getSchema(columnName, sqlType, sqlTypeName, columnClassName, precision, scale, isSigned);
    }
  }

  @Override
  public boolean shouldIgnoreColumn(ResultSetMetaData metadata, int index) throws SQLException {
    if (sessionID == null) {
      return false;
    }
    return metadata.getColumnName(index).equals("c_" + sessionID) ||
      metadata.getColumnName(index).equals("s_" + sessionID);
  }
}
