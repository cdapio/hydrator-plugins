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

import org.apache.hadoop.io.compress.SplitCompressionInputStream;

import java.io.IOException;

/**
 * Wrapper for the decompressor stream.
 */
public class TransformingCompressionInputStream extends SplitCompressionInputStream {

  private final FixedLengthCharsetTransformingDecompressorStream decompressorStream;

  public TransformingCompressionInputStream(FixedLengthCharsetTransformingDecompressorStream in, long start, long end)
    throws IOException {
    super(in, start, end);
    decompressorStream = in;
  }

  @Override
  public int read() throws IOException {
    return decompressorStream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return decompressorStream.read(b, off, len);
  }

  @Override
  public void resetState() throws IOException {
    decompressorStream.reset();
  }

  @Override
  public long getPos() throws IOException {
    return decompressorStream.getPos();
  }
}
