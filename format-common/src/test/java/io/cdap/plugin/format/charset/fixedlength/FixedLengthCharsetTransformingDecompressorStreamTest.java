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

package io.cdap.plugin.format.charset.fixedlength;

import io.cdap.plugin.format.charset.SeekableByteArrayInputStream;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class FixedLengthCharsetTransformingDecompressorStreamTest {

  FixedLengthCharsetTransformingDecompressorStream decompressorStream;
  SeekableByteArrayInputStream inputStream;

  String text = "abc\ndef\nghi\njkl\nmno\npqr\nwxy\nz12\n";

  @Before
  public void test() {
    inputStream = new SeekableByteArrayInputStream(text.getBytes(FixedLengthCharset.UTF_32.getCharset()));
  }

  @Test
  public void testGetCompressedDataPartitionBoundaries_FirstPartition() throws IOException {
    int bufferSize = 20;
    long partitionStart = 0;
    long partitionEnd = 32;

    decompressorStream = new FixedLengthCharsetTransformingDecompressorStream(inputStream,
                                                                              FixedLengthCharset.UTF_32,
                                                                              bufferSize,
                                                                              partitionStart,
                                                                              partitionEnd);

    int numReadBytes;

    //First read, limit not reached
    numReadBytes = decompressorStream.getCompressedData();
    assertEquals(20, numReadBytes);
    assertEquals(20, decompressorStream.getPos());

    //Second read, limit is reached in this batch.
    // Only 12 out of 20 possible bytes are read to reach the partition limit
    numReadBytes = decompressorStream.getCompressedData();
    assertEquals(12, numReadBytes);
    assertEquals(32, decompressorStream.getPos());

    //Additional read operation, buffer is now filled to completion.
    numReadBytes = decompressorStream.getCompressedData();
    assertEquals(20, numReadBytes);
    assertEquals(52, decompressorStream.getPos());
  }

  @Test
  public void testGetCompressedDataPartitionBoundaries_MiddlePartition() throws IOException {
    int bufferSize = 20;
    long partitionStart = 32;
    long partitionEnd = 64;

    decompressorStream = new FixedLengthCharsetTransformingDecompressorStream(inputStream,
                                                                              FixedLengthCharset.UTF_32,
                                                                              bufferSize,
                                                                              partitionStart,
                                                                              partitionEnd);

    int numReadBytes;

    //First read, limit not reached
    numReadBytes = decompressorStream.getCompressedData();
    assertEquals(20, numReadBytes);
    assertEquals(52, decompressorStream.getPos());

    //Second read, limit is reached in this batch.
    // Only 12 out of 20 possible bytes are read to reach the partition limit
    numReadBytes = decompressorStream.getCompressedData();
    assertEquals(12, numReadBytes);
    assertEquals(64, decompressorStream.getPos());

    //Additional read operation, buffer is now filled to completion.
    numReadBytes = decompressorStream.getCompressedData();
    assertEquals(20, numReadBytes);
    assertEquals(84, decompressorStream.getPos());
  }

  @Test
  public void testGetCompressedDataPartitionBoundaries_FinalPartition() throws IOException {
    int bufferSize = 20;
    long partitionStart = 96;
    long partitionEnd = 128;

    decompressorStream = new FixedLengthCharsetTransformingDecompressorStream(inputStream,
                                                                              FixedLengthCharset.UTF_32,
                                                                              bufferSize,
                                                                              partitionStart,
                                                                              partitionEnd);

    int numReadBytes;

    //First read, limit not reached
    numReadBytes = decompressorStream.getCompressedData();
    assertEquals(20, numReadBytes);
    assertEquals(116, decompressorStream.getPos());

    //Second read, limit is reached in this batch.
    // Only 12 out of 20 possible bytes are read to reach the partition limit
    numReadBytes = decompressorStream.getCompressedData();
    assertEquals(12, numReadBytes);
    assertEquals(128, decompressorStream.getPos());

    //Additional read operation, no more bytes can be read.
    numReadBytes = decompressorStream.getCompressedData();
    assertEquals(-1, numReadBytes);
    assertEquals(128, decompressorStream.getPos());
  }

  @Test
  public void testUsingDefaultDecompressor() throws IOException {

    //Create new decompressor and decompressor stream.
    //Notice we read from this compressor using the default read method.
    Decompressor decompressor = new FixedLengthCharsetTransformingDecompressor(FixedLengthCharset.UTF_32);
    DecompressorStream defaultDecompressorStream = new DecompressorStream(inputStream, decompressor, 20);

    int numReadBytes;

    //First read, Limit has exceeded boundary
    numReadBytes = defaultDecompressorStream.read(new byte[20], 0, 20);
    assertEquals(5, numReadBytes);
    assertEquals(defaultDecompressorStream.getPos(), 20);

    // Second read, as you can see, this read is already beyond our partition boundary. This will cause issues
    // at the LineRecordReader layer.
    numReadBytes = defaultDecompressorStream.read(new byte[20], 0, 20);
    assertEquals(5, numReadBytes);
    assertEquals(defaultDecompressorStream.getPos(), 40);
  }
}
