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

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;

public class FixedLengthCharsetTransformingDecompressorStreamTest {

  FixedLengthCharsetTransformingDecompressorStream stream;
  FixedLengthCharsetTransformingDecompressorStream decompressorStream;
  ByteArrayInputStream inputStream;
  ByteArrayOutputStream outputStream;

  String text = "abc\ndef\nghi\njkl\nmno\npqr\nwxy\nz12\n";

  @Before
  public void test() {
    inputStream = new ByteArrayInputStream(text.getBytes(Charset.forName("UTF-32")));
  }

  @Test
  public void testGetCompressedDataPartitionBoundaries_FirstPartition() throws IOException {
    int bufferSize = 20;
    byte[] buffer = new byte[bufferSize];
    long partitionStart = 0;
    long partitionEnd = 32;

    decompressorStream = new FixedLengthCharsetTransformingDecompressorStream(inputStream,
                                                                              FixedLengthCharset.UTF_32,
                                                                              partitionStart,
                                                                              partitionEnd);

    int numReadBytes;

    //First read, limit not reached
    numReadBytes = decompressorStream.read(buffer);
    assertEquals(numReadBytes, 5);
    assertEquals(decompressorStream.getPos(), 20);

    //Second read, limit is reached in this batch.
    // Only 12 out of 20 possible bytes are read to reach the partition limit
    numReadBytes = decompressorStream.read(buffer);
    assertEquals(numReadBytes, 3);
    assertEquals(decompressorStream.getPos(), 32);

    //Additional read operation, buffer is now filled to completion.
    numReadBytes = decompressorStream.read(buffer);
    assertEquals(numReadBytes, 5);
    assertEquals(decompressorStream.getPos(), 52);
  }

  @Test
  public void testGetCompressedDataPartitionBoundaries_MiddlePartition() throws IOException {
    int bufferSize = 20;
    byte[] buffer = new byte[bufferSize];
    long partitionStart = 32;
    long partitionEnd = 64;

    decompressorStream = new FixedLengthCharsetTransformingDecompressorStream(inputStream,
                                                                              FixedLengthCharset.UTF_32,
                                                                              partitionStart,
                                                                              partitionEnd);

    int numReadBytes;

    //First read, limit not reached
    numReadBytes = decompressorStream.read(buffer);
    assertEquals(numReadBytes, 5);
    assertEquals(decompressorStream.getPos(), 52);

    //Second read, limit is reached in this batch.
    // Only 12 out of 20 possible bytes are read to reach the partition limit
    numReadBytes = decompressorStream.read(buffer);
    assertEquals(numReadBytes, 3);
    assertEquals(decompressorStream.getPos(), 64);

    //Additional read operation, buffer is now filled to completion.
    numReadBytes = decompressorStream.read(buffer);
    assertEquals(numReadBytes, 5);
    assertEquals(decompressorStream.getPos(), 84);
  }

  @Test
  public void testGetCompressedDataPartitionBoundaries_FinalPartition() throws IOException {
    int bufferSize = 20;
    byte[] buffer = new byte[bufferSize];
    long partitionStart = 96;
    long partitionEnd = 128;

    decompressorStream = new FixedLengthCharsetTransformingDecompressorStream(inputStream,
                                                                              FixedLengthCharset.UTF_32,
                                                                              partitionStart,
                                                                              partitionEnd);

    int numReadBytes;

    //First read, limit not reached
    numReadBytes = decompressorStream.read(buffer);
    assertEquals(numReadBytes, 5);
    assertEquals(decompressorStream.getPos(), 116);

    //Second read, limit is reached in this batch.
    // Only 12 out of 20 possible bytes are read to reach the partition limit
    numReadBytes = decompressorStream.read(buffer);
    assertEquals(numReadBytes, 3);
    assertEquals(decompressorStream.getPos(), 128);

    //Additional read operation, no more bytes can be read.
    numReadBytes = decompressorStream.read(buffer);
    assertEquals(numReadBytes, -1);
    assertEquals(decompressorStream.getPos(), 128);
  }

}
