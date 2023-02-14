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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

/**
 * DB2 Schema reader.
 */
public class DB2SchemaReader extends CommonSchemaReader {

  public static final String DB2_DECFLOAT = "DECFLOAT";
  public static final Set<Integer> DB2_TYPES = ImmutableSet.of(Types.OTHER);

  @Override
  public Schema getSchema(String columnName, int sqlType, String sqlTypeName, String columnClassName,
                          int precision, int scale, boolean isSigned) throws SQLException {
    if (DB2_TYPES.contains(sqlType) && DB2_DECFLOAT.equals(sqlTypeName)) {
      return Schema.of(Schema.Type.STRING);
    } else {
      return super.getSchema(columnName, sqlType, sqlTypeName, columnClassName, precision, scale, isSigned);
    }
  }
}
