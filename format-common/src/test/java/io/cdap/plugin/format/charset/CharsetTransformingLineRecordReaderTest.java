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
import io.cdap.plugin.format.charset.fixedlength.FixedLengthCharsetTransformingDecompressorStream;
import io.cdap.plugin.format.charset.fixedlength.TransformingCompressionInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class CharsetTransformingLineRecordReaderTest {

  Configuration conf;
  FixedLengthCharset fixedLengthCharset;
  CharsetTransformingLineRecordReader recordReader;
  FixedLengthCharsetTransformingCodec codec;
  SeekableByteArrayInputStream inputStream;
  int availableBytes;
  FixedLengthCharsetTransformingDecompressorStream decompressorStream;
  SplitCompressionInputStream compressionInputStream;

  //We set up the input so each line is 5 characters long, which is 20 bytes in UTF-32.
  final String input = "abcd\nedfg\nijkl\n";

  @Before
  public void before() throws IOException {
    //Set up the Compressed Split Line Reader with a buffer size of 4096 bytes.
    //This ensures the buffer will consume all characters in the input stream if we allow it to.
    conf = new Configuration();
    conf.setInt("io.file.buffer.size", 4096);

    fixedLengthCharset = FixedLengthCharset.UTF_32;

    codec = new FixedLengthCharsetTransformingCodec(fixedLengthCharset);
    codec.setConf(conf);

    inputStream = new SeekableByteArrayInputStream(input.getBytes(fixedLengthCharset.getCharset()));
    availableBytes = inputStream.available();
  }

  public void setUpRecordReaderForTest(SplitCompressionInputStream splitCompressionInputStream) throws IOException {

    // Set up record reader to assume we'll read the file from the beggining, and the partition size is 32 bytes
    // which is 8 characters in UTF-32, meaning we expect to read the first 2 lines for this partition.
    recordReader = spy(new CharsetTransformingLineRecordReader(
      fixedLengthCharset,
      null,
      new CompressedSplitLineReader(splitCompressionInputStream, conf, null),
      0,
      0,
      32,
      4096
    ));

    //We will calculate position based on the number of bytes consumed from the input stream.
    doAnswer(a -> (long) availableBytes - inputStream.available()).when(recordReader).getFilePosition();
  }

  @Test
  public void testGetNextLine() throws IOException {
    decompressorStream =
      new FixedLengthCharsetTransformingDecompressorStream(inputStream, FixedLengthCharset.UTF_32, 0, 32);
    compressionInputStream = new TransformingCompressionInputStream(decompressorStream, 0, 32);

    //Set up test
    setUpRecordReaderForTest(compressionInputStream);

    //Ensure the first line is read
    Assert.assertTrue(recordReader.nextKeyValue());
    Assert.assertEquals(0, recordReader.getCurrentKey().get());
    Assert.assertEquals("abcd", recordReader.getCurrentValue().toString());
    //Ensure the second line is read
    Assert.assertTrue(recordReader.nextKeyValue());
    Assert.assertEquals(5, recordReader.getCurrentKey().get());
    Assert.assertEquals("edfg", recordReader.getCurrentValue().toString());
    //Ensure no more lines can be read.
    Assert.assertFalse(recordReader.nextKeyValue());
    Assert.assertNull(recordReader.getCurrentKey());
    Assert.assertNull(recordReader.getCurrentValue());
    Assert.assertFalse(recordReader.nextKeyValue());
    Assert.assertNull(recordReader.getCurrentKey());
    Assert.assertNull(recordReader.getCurrentValue());
  }

  @Test
  public void testGetNextLineCallingOriginalGetCompressedDataMethod() throws IOException {
    //We'll override the getCompressedData method by invoking the superclass getCompressedData method.
    //This will highlight the partition boundary issues we had to solve.
    CompressionInputStream defaultCompressorInputStream =
      codec.createInputStream(inputStream, codec.createDecompressor());
    SplitCompressionInputStream defaultSplitCompressionInputStream =
      new SplitCompressionInputStream(defaultCompressorInputStream, 0, 32) {
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          return in.read(b, off, len);
        }

        @Override
        public void resetState() throws IOException {
          in.reset();
        }

        @Override
        public int read() throws IOException {
          return in.read();
        }
      };

    //Set up test
    setUpRecordReaderForTest(defaultSplitCompressionInputStream);

    //Ensure first line is read
    Assert.assertTrue(recordReader.nextKeyValue());
    Assert.assertEquals(0, recordReader.getCurrentKey().get());
    Assert.assertEquals("abcd", recordReader.getCurrentValue().toString());
    //Notice that the next line does not get read, as the file position goes beyond the partition boundary, even when
    //not all expected lines have been read yet.
    Assert.assertFalse(recordReader.nextKeyValue());
    Assert.assertNull(recordReader.getCurrentKey());
    Assert.assertNull(recordReader.getCurrentValue());
    Assert.assertFalse(recordReader.nextKeyValue());
    Assert.assertNull(recordReader.getCurrentKey());
    Assert.assertNull(recordReader.getCurrentValue());
  }
}
