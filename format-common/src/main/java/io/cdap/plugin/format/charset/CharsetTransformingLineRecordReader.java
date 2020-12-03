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

package io.cdap.plugin.format.charset;

import io.cdap.plugin.format.charset.fixedlength.FixedLengthCharset;
import io.cdap.plugin.format.charset.fixedlength.FixedLengthCharsetTransformingCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Copy of Hadoop's Line Record Reader (Hadoop Version 2.3.0)
 * <p>
 * This class uses a fixed Codec and Decompressor to parse records.
 */
public class CharsetTransformingLineRecordReader extends LineRecordReader {
  private static final Logger LOG = LoggerFactory.getLogger(CharsetTransformingLineRecordReader.class);
  public static final String MAX_LINE_LENGTH =
    "mapreduce.input.linerecordreader.line.maxlength";

  private final FixedLengthCharset fixedLengthCharset;
  private long start;
  private long pos;
  private long end;
  private SplitLineReader in;
  private FSDataInputStream fileIn;
  private Seekable filePosition;
  private int maxLineLength;
  private LongWritable key;
  private Text value;
  private boolean isCompressedInput;
  private Decompressor decompressor;
  private byte[] recordDelimiterBytes;

  public CharsetTransformingLineRecordReader(FixedLengthCharset fixedLengthCharset) {
    this.fixedLengthCharset = fixedLengthCharset;
  }

  public CharsetTransformingLineRecordReader(FixedLengthCharset fixedLengthCharset, byte[] recordDelimiter) {
    this.fixedLengthCharset = fixedLengthCharset;
    this.recordDelimiterBytes = recordDelimiter;
  }

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();

    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);

    SplittableCompressionCodec codec = new FixedLengthCharsetTransformingCodec(fixedLengthCharset);
    isCompressedInput = true;
    decompressor = codec.createDecompressor();

    final SplitCompressionInputStream cIn =
      codec.createInputStream(
        fileIn, decompressor, start, end,
        SplittableCompressionCodec.READ_MODE.CONTINUOUS);
    in = new CompressedSplitLineReader(cIn, job,
                                       this.recordDelimiterBytes);
    start = cIn.getAdjustedStart();
    end = cIn.getAdjustedEnd();
    filePosition = cIn;

    // If this is not the first split, we always throw away first record
    // because we always (except the last split) read one extra line in
    // next() method.
    if (start != 0) {
      Text t = new Text();
      start += in.readLine(t, 4096, maxBytesToConsume(start));
      LOG.info("Discarded line: " + t.toString());
    }
    this.pos = start;
  }


  private int maxBytesToConsume(long pos) {
    return isCompressedInput
      ? Integer.MAX_VALUE
      : (int) Math.min(Integer.MAX_VALUE, end - pos);
  }

  private long getFilePosition() throws IOException {
    LOG.info("File position is: {}", filePosition.getPos());
    return filePosition.getPos();
  }

  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    // We always read one extra line, which lies outside the upper
    // split limit i.e. (end - 1)
    while (getFilePosition() <= end || in.needAdditionalRecordAfterSplit()) {
      newSize = in.readLine(value, maxLineLength,
                            Math.max(maxBytesToConsume(pos), maxLineLength));
      pos += newSize;
      if (newSize < maxLineLength) {
        break;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " +
                 (pos - newSize));
    }
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
    }
  }

  public synchronized void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}
