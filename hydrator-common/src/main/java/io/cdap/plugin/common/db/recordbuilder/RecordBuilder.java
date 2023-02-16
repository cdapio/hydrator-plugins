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

package io.cdap.plugin.common.db.recordbuilder;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Record Reader methods which contains the logic to create a single StructuredRecord from the given
 * ResultSet and Schema. And also provide a way to deduce the amount of bytes read in the process.
 */
public interface RecordBuilder {

    /**
     * Create the StructuredRecord.Builder instance using the given ResultSet and the Schema.
     * This method internally call handleField method which can be used to implement a special
     * handling logic for any given Database.
     * This method is intended to work on the current pointed Result Row in the result set.
     * This method creates the Builder instead of the StructuredRecord to allow the callers to set
     * custom fields on the Builder beside the ones which are present in the given Schema.
     * For example. Multi Table BatchSource Plugin have a use case to set the Table Name in the StructuredRecord.
     *
     * @param resultSet ResultSet for a given query
     * @param schema Schema of the StructuredRecord to be created
     * @return Builder instance which can be used to create StructuredRecord.
     * @throws SQLException In case of any error while reading the data from the given ResultSet.
     */
    StructuredRecord.Builder getRecordBuilder(ResultSet resultSet, Schema schema) throws SQLException;

    /**
     * Returns the number of bytes read for a single record.
     * @return The amount of bytes read in long.
     */
    long getBytesRead();
}
