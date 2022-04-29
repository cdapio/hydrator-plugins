/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.format;

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * This class is wrapper for file metadata (file length and modification time).
 * @param <KEYIN>
 * @param <VALUEIN>
 */
public abstract class MetadataRecordReader<KEYIN, VALUEIN> extends RecordReader<KEYIN, VALUEIN> {
    private long length;
    private long modificationTime;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        Path file = ((FileSplit) split).getPath();
        final FileSystem fs = file.getFileSystem(context.getConfiguration());
        final FileStatus fileStatus = fs.getFileStatus(file);

        length = fileStatus.getLen();
        modificationTime = fileStatus.getModificationTime();
    }

    public long getLength() {
        return length;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public void populateMetadata(String fieldName, MetadataField metadataField,
                                 StructuredRecord.Builder recordBuilder) {
        Object result;
        switch (metadataField) {
            case FILE_LENGTH:
                result = getLength();
                break;
            case FILE_MODIFICATION_TIME:
                result = getModificationTime();
                break;
            default:
                throw new IllegalArgumentException("Unable to populate metadata field: " + fieldName);
        }
        recordBuilder.set(fieldName, result);
    }
}
