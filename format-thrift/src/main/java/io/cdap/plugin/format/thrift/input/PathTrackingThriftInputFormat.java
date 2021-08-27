/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.plugin.format.thrift.input;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.annotation.Nullable;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.thrift.ThriftReadSupport;
import org.apache.thrift.TBase;

/**
 * Text format that tracks which file each record was read from.
 */
public class PathTrackingThriftInputFormat extends PathTrackingInputFormat {

    public PathTrackingThriftInputFormat() {
    }

    @Override
    protected RecordReader<NullWritable, StructuredRecord.Builder> createRecordReader(FileSplit split,
                                                                                      TaskAttemptContext context,
                                                                                      @Nullable String pathField,
                                                                                      Schema schema) {
//        RecordReader<LongWritable, Text> delegate = getDefaultRecordReaderDelegate(split, context);
        RecordReader<Void, TBase> delegate = new ParquetRecordReader<>(new ThriftReadSupport<>());
        return new ThriftRecordReader(delegate, schema);
    }
}
