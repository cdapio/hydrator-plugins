/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch;

import net.sf.JRecord.Common.AbstractFieldValue;
import net.sf.JRecord.Details.AbstractLine;
import net.sf.JRecord.External.Def.ExternalField;
import net.sf.JRecord.External.ExternalRecord;
import net.sf.JRecord.IO.AbstractLineReader;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static co.cask.hydrator.plugin.batch.CopybookSource.COPYBOOK_INPUTFORMAT_CBL_CONTENTS;
import static co.cask.hydrator.plugin.batch.CopybookSource.COPYBOOK_INPUTFORMAT_DATA_HDFS_PATH;
import static co.cask.hydrator.plugin.batch.CopybookSource.COPYBOOK_INPUTFORMAT_FILE_STRUCTURE;
import static co.cask.hydrator.plugin.batch.CopybookSource.DEFAULT_FILE_STRUCTURE;

/**
 * Record Reader for CopybookReader plugin.
 * <p>
 * This will return the field name and value using the binary data and Copybook contents, to be used as the transform
 * method input
 */
public class CopybookRecordReader extends RecordReader<LongWritable, Map<String, String>> {

  private AbstractLineReader reader;
  private ExternalRecord externalRecord;
  private int recordByteLength;
  private long start;
  private long position;
  private long end;
  private LongWritable key = null;
  private Map<String, String> value = null;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    // Get configuration
    Configuration conf = context.getConfiguration();
    int fileStructure = Integer.parseInt(conf.get(COPYBOOK_INPUTFORMAT_FILE_STRUCTURE,
                                                  Integer.toString(DEFAULT_FILE_STRUCTURE)));
    Path path = new Path(conf.get(COPYBOOK_INPUTFORMAT_DATA_HDFS_PATH));
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    //create input stream for the cobol Copybook contents
    InputStream inputStream = IOUtils.toInputStream(conf.get(COPYBOOK_INPUTFORMAT_CBL_CONTENTS), "UTF-8");
    BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
    try {
      externalRecord = CopybookIOUtils.getExternalRecord(bufferedInputStream);
      recordByteLength = CopybookIOUtils.getRecordLength(externalRecord, fileStructure);
      org.apache.hadoop.mapreduce.lib.input.FileSplit fileSplit =
        (org.apache.hadoop.mapreduce.lib.input.FileSplit) split;

      start = fileSplit.getStart();
      end = start + fileSplit.getLength();

      BufferedInputStream fileIn = new BufferedInputStream(fs.open(fileSplit.getPath()));
      /* Jump to the point in the split at which the first
       whole record of split starts if not the first InputSplit*/
      if (start != 0) {
        position = start - (start % recordByteLength) + recordByteLength;
        fileIn.skip(position);
      }
      reader = CopybookIOUtils.getAndOpenLineReader(fileIn, fileStructure, externalRecord);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    AbstractLine line = reader.read();
    if (line == null) {
      return false;
    }
    if (position > end) {
      return false;
    }
    if (key == null) {
      key = new LongWritable();
    }
    value = new HashMap();
    position += recordByteLength;
    key.set(position);
    for (ExternalField field : externalRecord.getRecordFields()) {
      /*check if field is binary
       in case of binary fields encode using Base64 format to preseve data consistency*/
      // TODO : check int value for data types returned by jrecord to match java data types
      // and create schema accrodindly
      AbstractFieldValue filedValue = line.getFieldValue(field.getName());
      if (filedValue.isBinary()) {
        value.put(field.getName(), new String(Base64.decodeBase64(Base64.encodeBase64String(
          filedValue.toString().getBytes()))));
      } else {
        value.put(field.getName(), filedValue.toString());
      }
    }
    key.set(position);
    return true;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Map<String, String> getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (position - start) / (float) (end - start));
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
