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

package io.cdap.plugin.common.db.dbrecordwriter;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.validation.InvalidStageException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * DB2 Record Writer Helper
 */
public class DB2RecordWriterHelper extends CommonRecordWriterHelper {

    private static final int ILLEGAL_CONVERSION_ERROR_CODE = -4474;

    @Override
    public void write(PreparedStatement stmt,
                      StructuredRecord record,
                      List<ColumnType> columnTypes) throws SQLException {
        // DB2 driver throws SQLException if data conversation fails, but SQLException is skipped.
        // So we need to throw another exception to fail pipeline in this case.
        try {
            super.write(stmt, record, columnTypes);
        } catch (SQLException e) {
            if (e.getErrorCode() == ILLEGAL_CONVERSION_ERROR_CODE) {
                throw new InvalidStageException(e.getMessage(), e);
            } else {
                throw e;
            }
        }
    }
}
