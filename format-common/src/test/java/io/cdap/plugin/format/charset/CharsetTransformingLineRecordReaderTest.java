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
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.doCallRealMethod;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

public class CharsetTransformingLineRecordReaderTest {

  CharsetTransformingLineRecordReader recordReader;
  FixedLengthCharsetTransformingCodec codec;
  ByteArrayInputStream inputStream;
  int availableBytes;
  FixedLengthCharsetTransformingDecompressorStream decompressorStream;
  SplitCompressionInputStream compressionInputStream;

  //We set up the input so each line is 5 characters long, which is 20 bytes in UTF-32.
  final String input = "abcd\nedfg\nijkl\n";

  @Before
  public void before() throws IOException {
    recordReader = Mockito.mock(CharsetTransformingLineRecordReader.class);

    codec = spy(new FixedLengthCharsetTransformingCodec(FixedLengthCharset.UTF_32));

    inputStream =new ByteArrayInputStream(input.getBytes(FixedLengthCharset.UTF_32.getCharset()));
    availableBytes = inputStream.available();


  }

  public void setUpRecordReaderForTest() throws IOException {
    //Set up the Compressed Split Line Reader with a buffer size of 4096 bytes.
    //This ensures the buffer will consume all characters in the input stream if we allow it to.
    Configuration conf = Mockito.mock(Configuration.class);
    when(conf.getInt(anyString(), anyInt())).thenReturn(4096);

    // Set up record reader to assume we'll read the file from the beggining, and the partition size is 32 bytes
    // which is 8 characters in UTF-32, meaning we expect to read the first 2 lines for this partition.
    recordReader.in = new CompressedSplitLineReader(compressionInputStream, conf, null);
    recordReader.start = 0;
    recordReader.pos = 0;
    recordReader.end = 32;
    recordReader.key = null;
    recordReader.value = null;
    recordReader.maxLineLength = 4096;
    doCallRealMethod().when(recordReader).nextKeyValue();
    //We will calculate position based on the number of bytes consumed from the input stream.
    when(recordReader.getFilePosition()).thenAnswer(a -> (long) availableBytes - inputStream.available());
  }

  @Test
  public void testGetNextLine() throws IOException {
    decompressorStream =
      new FixedLengthCharsetTransformingDecompressorStream(inputStream, FixedLengthCharset.UTF_32, 0, 32);
    compressionInputStream = new TransformingCompressionInputStream(decompressorStream, 0, 32);

    //Set up test
    setUpRecordReaderForTest();

    //Ensure the first line is read
    recordReader.nextKeyValue();
    Assert.assertEquals(recordReader.key.get(), 0);
    Assert.assertEquals(recordReader.value.toString(), "abcd");
    //Ensure the second line is read
    recordReader.nextKeyValue();
    Assert.assertEquals(recordReader.key.get(), 5);
    Assert.assertEquals(recordReader.value.toString(), "edfg");
    //Ensure no more lines can be read.
    recordReader.nextKeyValue();
    Assert.assertNull(recordReader.key);
    Assert.assertNull(recordReader.value);
    recordReader.nextKeyValue();
    Assert.assertNull(recordReader.key);
    Assert.assertNull(recordReader.value);
  }

  @Test
  public void testGetNextLineCallingOriginalGetCompressedDataMethod() throws IOException {
    //We'll override the getCompressedData method by invoking the superclass getCompressedData method.
    //This will highlight the partition boundary issues we had to solve.
    decompressorStream =
      new FixedLengthCharsetTransformingDecompressorStream(inputStream, FixedLengthCharset.UTF_32, 0, 32) {
        @Override
        protected int getCompressedData() throws IOException {
          return super.getCompressedDataSuper();
        }
      };
    compressionInputStream = new TransformingCompressionInputStream(decompressorStream, 0, 32);

    //Set up test
    setUpRecordReaderForTest();

    //Ensure first line is read
    recordReader.nextKeyValue();
    Assert.assertEquals(recordReader.key.get(), 0);
    Assert.assertEquals(recordReader.value.toString(), "abcd");
    //Notice that the next line does not get read, as the file position goes beyond the partition boundary, even when
    //not all expected lines have been read yet.
    recordReader.nextKeyValue();
    Assert.assertNull(recordReader.key);
    Assert.assertNull(recordReader.value);
    recordReader.nextKeyValue();
    Assert.assertNull(recordReader.key);
    Assert.assertNull(recordReader.value);
  }
}
