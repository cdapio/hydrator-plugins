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

package io.cdap.plugin.common.db.recordwriter;

import io.cdap.cdap.api.data.format.StructuredRecord;

import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Record Reader methods which write a StructuredRecord
 */
public interface RecordWriter {

    /**
     * Write the given StructuredRecord into the DataOutput stream using the
     * Schema present in the record instance.
     * @param out DataOutput instance to write into
     * @param record StructuredRecord instance to be written
     * @throws IOException In case of I/O error while writing into DataOutput
     */
    void write(DataOutput out, StructuredRecord record) throws IOException;

    /**
     * Write the given StructuredRecord fields into the PreparedStatement instance using the
     * List of ColumnType which provide the SQL type of each of the fields.
     *
     * @param statement Statement instance to be executed to write into DB
     * @param record StructuredRecord instance to be written into DB
     * @param columnTypes ColumnType containing list of SQL type of all fields
     * @throws SQLException In case any SQL type is not supported
     * or if there is any non-compatible type getting set in the given PreparedStatement instance.
     */
    void write(PreparedStatement statement, StructuredRecord record, List<ColumnType> columnTypes) throws SQLException;

    /**
     * This method provides the amount bytes written during the write operation of a single StructuredRecord to DB.
     * @return Long value of total bytes written in the DB.
     */
    long getBytesWritten();
}
