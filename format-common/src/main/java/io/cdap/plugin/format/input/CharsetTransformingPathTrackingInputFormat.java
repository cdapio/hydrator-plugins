/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.format.input;

import io.cdap.plugin.format.charset.CharsetTransformingLineRecordReader;
import io.cdap.plugin.format.charset.fixedlength.FixedLengthCharset;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * An input format that tracks which the file path each record was read from. This InputFormat is a wrapper around
 * underlying input formats. The responsibility of this class is to keep track of which file each record is reading
 * from, and to add the file URI to each record. In addition, for text files, it can be configured to keep track
 * of the header for the file, which underlying record readers can use.
 */
public class CharsetTransformingPathTrackingInputFormat extends TextInputFormat {

  protected final FixedLengthCharset fixedLengthCharset;

  public CharsetTransformingPathTrackingInputFormat(String charsetName) {
    this.fixedLengthCharset = FixedLengthCharset.forName(charsetName);
  }

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    String delimiter = context.getConfiguration().get("textinputformat.record.delimiter");
    byte[] recordDelimiterBytes = null;
    if (null != delimiter) {
      recordDelimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);
    }
    return new CharsetTransformingLineRecordReader(fixedLengthCharset, recordDelimiterBytes);
  }

}
