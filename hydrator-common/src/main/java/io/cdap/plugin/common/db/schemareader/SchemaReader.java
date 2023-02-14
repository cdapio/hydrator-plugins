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

import io.cdap.cdap.api.data.schema.Schema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Main Interface to read db specific types.
 */
public interface SchemaReader {

    /**
     * Given the result set, get the metadata of the result set and return
     * list of {@link io.cdap.cdap.api.data.schema.Schema.Field},
     * where name of the field is same as column name and type of the field is obtained using
     * {@link SchemaReader#getSchema(ResultSetMetaData, int)}
     *
     * @param resultSet result set of executed query
     * @param regexPattern the regular expression to which the field name string is to be matched
     * @param replacement the string to be substituted in the field Name for each match
     * @return list of schema fields
     * @throws SQLException
     */
    List<Schema.Field> getSchemaFields(ResultSet resultSet,
                                       String regexPattern,
                                       String replacement) throws SQLException;

    /**
     * Given a sql metadata return schema type
     * @param metadata resultSet metadata
     * @param index column index
     * @return CDAP schema
     * @throws SQLException
     */
    Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException;

    /**
     * Given a sql metadata return schema type
     *
     * @param columnName      Name of the column in the ResultSet
     * @param sqlType         SQL type in the database
     * @param sqlTypeName     SQL type name in the database
     * @param columnClassName Column Class Name for the Column
     * @param precision       Precision of the Column
     * @param scale           Scale of the Column
     * @param isSigned        True in case the field is signed, false other wise.
     * @return CDAP schema
     * @throws SQLException
     */
    Schema getSchema(String columnName, int sqlType, String sqlTypeName, String columnClassName,
                     int precision, int scale, boolean isSigned) throws SQLException;

    /**
     * Given a sql metadata indicates if sql column must be ignored and not included in the output schema. Thus we can
     * support different data types for Sink and Source plugins.
     * <p/>
     * For example, in MS SQL 'TIMESTAMP' is the synonym for the 'ROWVERSION' data type, values of which are
     * automatically generated and can not be inserted or updated. Therefore 'TIMESTAMP' can not be supported by Sink
     * plugin. 'TIMESTAMP' reported as non-nullable column by JDBC connector, so inferred schema will be also
     * non-nullable for this field, requiring to set a value. By ignoring this column we will avoid schema validation
     * failure.
     * @param metadata resultSet metadata
     * @param index sql column index
     * @return 'true' if sql column must not included in the output schema, 'false' otherwise
     * @throws SQLException
     */
    boolean shouldIgnoreColumn(ResultSetMetaData metadata, int index) throws SQLException;
}
