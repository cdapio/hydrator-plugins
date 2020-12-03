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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Decompressor that can be used to convert byte streams in fixed-length character encodings to a stream of UTF-8 bytes.
 */
public class FixedLengthCharsetTransformingDecompressor implements Decompressor {

  final FixedLengthCharset origin;
  final Charset destination = StandardCharsets.UTF_8;
  long numConsumedBytes = 0;

  ByteArrayOutputStream incomingBuffer = new ByteArrayOutputStream();
  ByteArrayInputStream outgoingBuffer = new ByteArrayInputStream(new byte[]{});

  public FixedLengthCharsetTransformingDecompressor(FixedLengthCharset origin) {
    this.origin = origin;
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
    //Append the newly received bytes into the Input buffer
    incomingBuffer.write(b, off, len);
    byte[] input = incomingBuffer.toByteArray();

    //Count how many bytes we have consumed so far.
    numConsumedBytes += len;

    //Calculate the position in this array where the last full character can be decoded.
    int length = input.length;
    int lastCharacterBoundary = (input.length / origin.getCharLength()) * origin.getCharLength();

    //Decode the incoming bytes as a string.
    String inputString = new String(input, 0, lastCharacterBoundary, origin.getCharset());
    //Encode the previously decoded string as UTF-8 bytes and add this payload into the Outgoing buffer to serve reads.
    outgoingBuffer = new ByteArrayInputStream(inputString.getBytes(destination));

    //Clean up the input buffer.
    incomingBuffer.reset();

    //If there are remaining bytes that do not align to a full character, append those bytes into the input buffer
    // so they form a full character in a subsequent invocation of this function.
    if (length > lastCharacterBoundary) {
      incomingBuffer.write(input, lastCharacterBoundary, length - lastCharacterBoundary);
    }

  }

  /**
   * Note that we only ask for additional input once we have completely depleted out outgoing buffer.
   */
  @Override
  public boolean needsInput() {
    return outgoingBuffer.available() == 0;
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    //no-op
  }

  @Override
  public boolean needsDictionary() {
    return false;
  }

  @Override
  public boolean finished() {
    return outgoingBuffer.available() == 0;
  }

  @Override
  public int decompress(byte[] b, int off, int len) throws IOException {
    return outgoingBuffer.read(b, off, len);
  }

  @Override
  public int getRemaining() {
    return incomingBuffer.size();
  }

  @Override
  public void reset() {
    incomingBuffer.reset();
    outgoingBuffer.reset();
  }

  @Override
  public void end() {
    this.reset();
  }

  public long getNumConsumedBytes() {
    return this.numConsumedBytes;
  }
}
