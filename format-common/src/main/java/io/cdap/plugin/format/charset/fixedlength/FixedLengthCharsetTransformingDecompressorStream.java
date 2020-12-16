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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * DecompressorStream implementation for the FixedLengthCharsetTransformingDecompressor.
 * <p>
 * This can be used to convert input streams containing bytes for fixed length charsets into UTF-8 bytes.
 */
public class FixedLengthCharsetTransformingDecompressorStream extends DecompressorStream {
  private static final Logger LOG = LoggerFactory.getLogger(FixedLengthCharsetTransformingDecompressorStream.class);

  //Starting and ending position in the file.
  protected final long start;
  protected final long end;
  protected final FixedLengthCharset fixedLengthCharset;
  protected long totalReadBytes = 0;

  public FixedLengthCharsetTransformingDecompressorStream(InputStream in,
                                                             FixedLengthCharset fixedLengthCharset,
                                                             long start,
                                                             long end)
    throws IOException {
    super(in, new FixedLengthCharsetTransformingDecompressor(fixedLengthCharset), 4096);
    in.skip(start);
    this.fixedLengthCharset = fixedLengthCharset;
    this.start = start;
    this.end = end;
  }

  @VisibleForTesting
  protected FixedLengthCharsetTransformingDecompressorStream(InputStream in,
                                                             FixedLengthCharset fixedLengthCharset,
                                                             int bufferSize,
                                                             long start,
                                                             long end)
    throws IOException {
    super(in, new FixedLengthCharsetTransformingDecompressor(fixedLengthCharset), bufferSize);
    in.skip(start);
    this.fixedLengthCharset = fixedLengthCharset;
    this.start = start;
    this.end = end;
  }

  /**
   * Fill the input buffer with data from the source input.
   * <p>
   * Partition boundaries present a challenge: We need to make sure to read just enough characters to make it into
   * the next complete line.
   * <p>
   * The way we accomplish this is to keep track of how many bytes we have read from the source stream. It the next
   * invocation of this method reaches or goes over the partition boundary, we align the read operation to the
   * partition boundary.
   * <p>
   * This ensures the caller method will either read a full line, or call this method once again to ensure the final
   * line of the given partition is read and not a single extra line.
   *
   * @return Number of bytes read from the source.
   * @throws IOException when there is a problema reading from the underlying stream.
   */
  @Override
  protected int getCompressedData() throws IOException {
    checkStream();

    int len = buffer.length;

    // Make sure we decompress up to the partition boundary when the next read operation reaches this position.
    // This ensures our decompression is aligned to the partition boundary and the next line that is read is the
    // final line for this partition.
    if (start + totalReadBytes < end && start + totalReadBytes + len > end) {
      //Ensure we only decompress up to the partition boundary if we are approaching this limit.
      long bytesUntilPartitionBoundary = end - (start + totalReadBytes);

      if (bytesUntilPartitionBoundary < buffer.length) {
        len = (int) bytesUntilPartitionBoundary;
      }
    }

    int numReadBytes = in.read(buffer, 0, len);

    totalReadBytes += numReadBytes;

    return numReadBytes;
  }

  /**
   * Method that invokes the parent method to get decompressed data, without partition boundary awareness.
   * <p>
   * This method is visible for the purposes of testing the partition boundaries.
   *
   * @return number of bytes read from source input stream
   * @throws IOException If there was a problem reading from the underlying input stream.
   */
  @VisibleForTesting
  protected int getCompressedDataSuper() throws IOException {
    return super.getCompressedData();
  }
}
