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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * SQL Server Sink schema reader.
 */
public class SqlServerSinkSchemaReader extends SqlServerSourceSchemaReader {

  @Override
  public boolean shouldIgnoreColumn(ResultSetMetaData metadata, int index) throws SQLException {
    // Ignore 'TIMESTAMP' column in the output schema since values of this type are generated automatically and can
    // not be set nor updated
    return TIMESTAMP_TYPE_NAME.equalsIgnoreCase(metadata.getColumnTypeName(index));
  }
}
