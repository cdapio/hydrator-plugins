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

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * DecompressorStream implementation for the FixedLengthCharsetTransformingDecompressor.
 * <p>
 * This can be used to convert input streams containing bytes for fixed length charsets into UTF-8 bytes.
 */
public class FixedLengthCharsetTransformingDecompressorStream extends SplitCompressionInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(FixedLengthCharsetTransformingDecompressorStream.class);

  protected final FixedLengthCharset fixedLengthCharset;
  protected Decompressor decompressor;

  public FixedLengthCharsetTransformingDecompressorStream(InputStream in,
                                                             FixedLengthCharset fixedLengthCharset,
                                                             long start,
                                                             long end)
    throws IOException {
    super(in, start, end);
    in.skip(start);
    this.fixedLengthCharset = fixedLengthCharset;
    decompressor = new FixedLengthCharsetTransformingDecompressor(this.fixedLengthCharset);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int decoded = decompressor.decompress(b, off, len);
    if (decoded > 0) {
      return decoded;
    }
    //Now we need to read more data. Ensure we always stop at block boundary. This would ensure no dup / missing records
    long lenToEndOfSplit = getAdjustedEnd() - getPos();
    int toRead = (lenToEndOfSplit > 0 && lenToEndOfSplit < len) ? (int) lenToEndOfSplit : len;

    int sourceBytesRead = in.read(b, off, toRead);
    if (sourceBytesRead <= 0) {
      //EOF
      return sourceBytesRead;
    }
    decompressor.setInput(b, off, sourceBytesRead);
    return decompressor.decompress(b, off, len);
  }

  @Override
  public void resetState() throws IOException {
    decompressor.reset();
  }

  public int read() throws IOException {
    byte b[] = new byte[1];
    int result = this.read(b, 0, 1);
    return (result < 0) ? result : (b[0] & 0xff);
  }
}
