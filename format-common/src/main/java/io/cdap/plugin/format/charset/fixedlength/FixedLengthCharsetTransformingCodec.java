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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Codec implementation that returns a decompressor for Fixed Length character encodings.
 */
public class FixedLengthCharsetTransformingCodec extends DefaultCodec
  implements Configurable, SplittableCompressionCodec {
  private static final Logger LOG = LoggerFactory.getLogger(FixedLengthCharsetTransformingCodec.class);

  private final FixedLengthCharset sourceEncoding;

  public FixedLengthCharsetTransformingCodec(FixedLengthCharset sourceEncoding) {
    this.sourceEncoding = sourceEncoding;
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public Compressor createCompressor() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return FixedLengthCharsetTransformingDecompressor.class;
  }

  @Override
  public DirectDecompressor createDirectDecompressor() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public String getDefaultExtension() {
    return ".*";
  }

  @Override
  public Decompressor createDecompressor() {
    return new FixedLengthCharsetTransformingDecompressor(sourceEncoding);
  }

  public SplitCompressionInputStream createInputStream(InputStream seekableIn,
                                                       Decompressor decompressor,
                                                       long start,
                                                       long end,
                                                       READ_MODE readMode) throws IOException {
    if (!(seekableIn instanceof Seekable)) {
      throw new IOException("seekableIn must be an instance of " +
                              Seekable.class.getName());
    }

    //Adjust start to align to the next character boundary.
    if (start % sourceEncoding.getNumBytesPerCharacter() != 0) {
      long adjustment = sourceEncoding.getNumBytesPerCharacter() - (start % sourceEncoding.getNumBytesPerCharacter());
      LOG.trace("Adjusted partition start positioon from {} to {} by {} bytes",
                start, start + adjustment, adjustment);
      start += adjustment;
    }

    //Adjust end to align to the next character boundary.
    if (end % sourceEncoding.getNumBytesPerCharacter() != 0) {
      long adjustment = sourceEncoding.getNumBytesPerCharacter() - (end % sourceEncoding.getNumBytesPerCharacter());
      LOG.trace("Adjusted partition end position from {} to {} by {} bytes",
                end, end + adjustment, adjustment);
      end += adjustment;
    }

    FixedLengthCharsetTransformingDecompressorStream decompressorStream =
      new FixedLengthCharsetTransformingDecompressorStream(seekableIn, sourceEncoding, start, end);

    return new TransformingCompressionInputStream(decompressorStream, start, end);
  }

}
