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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;

/**
 * Decompressor that can be used to convert byte streams in fixed-length character encodings to a stream of UTF-8 bytes.
 */
public class FixedLengthCharsetTransformingDecompressor implements Decompressor {

  private static final Logger LOG = LoggerFactory.getLogger(FixedLengthCharsetTransformingDecompressor.class);

  private final FixedLengthCharset sourceEncoding;
  private final CharsetDecoder decoder;
  private final CharsetEncoder encoder;
  private final Charset targetCharset = StandardCharsets.UTF_8;

  //Initializing all buffers.
  private ByteBuffer inputByteBuffer = ByteBuffer.allocate(0);
  private CharBuffer decodedCharBuffer = CharBuffer.allocate(0);
  private ByteBuffer partialOutputByteBuffer = ByteBuffer.allocate(0);

  public FixedLengthCharsetTransformingDecompressor(FixedLengthCharset sourceEncoding) {
    this.sourceEncoding = sourceEncoding;
    this.decoder = sourceEncoding.getCharset().newDecoder();
    this.encoder = targetCharset.newEncoder();
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
    //Set up incoming buffer for writes.
    inputByteBuffer.compact();

    //Expand incoming buffer if needed.
    if (inputByteBuffer.remaining() < len) {
      //Allocate new buffer that can fill the existing input + newly received bytes
      ByteBuffer newIncomingBuffer = ByteBuffer.allocate(len + inputByteBuffer.capacity());

      //Set up incoming buffer for reads and copy contents into new buffer.
      inputByteBuffer.flip();
      newIncomingBuffer.put(inputByteBuffer);

      inputByteBuffer = newIncomingBuffer;
    }

    //Copy incoming payload into Input Byte Buffer
    inputByteBuffer.put(b, off, len);
    inputByteBuffer.flip();

    //Set up char buffer for writes
    decodedCharBuffer.compact();

    //Expand the char buffer if needed.
    if (decodedCharBuffer.capacity() < inputByteBuffer.limit() / sourceEncoding.getNumBytesPerCharacter()) {
      decodedCharBuffer = CharBuffer.allocate(inputByteBuffer.limit() / sourceEncoding.getNumBytesPerCharacter());
    }

    //Decode bytes from the input buffer into the Decoded Char Buffer
    decodeByteBufferIntoCharBuffer(inputByteBuffer);

    //Set up decoded char buffer for reads.
    decodedCharBuffer.flip();

  }

  /**
   * Note that we only ask for additional input once we have completely depleted out outgoing buffer.
   */
  @Override
  public boolean needsInput() {
    return decodedCharBuffer.remaining() == 0;
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
    return decodedCharBuffer.remaining() == 0 && partialOutputByteBuffer.remaining() == 0;
  }

  @Override
  public int decompress(byte[] b, int off, int len) throws IOException {

    //Allocate new outgoing buffer
    ByteBuffer encodedBuffer = ByteBuffer.wrap(b, off, len);

    //Consume any remaining bytes from a previous decompress invocation.
    while (partialOutputByteBuffer != null && partialOutputByteBuffer.hasRemaining() && encodedBuffer.hasRemaining()) {
      encodedBuffer.put(partialOutputByteBuffer.get());
    }

    //Encode as many characters as possible into the Encoded Buffer.
    encodeCharBufferIntoByteBuffer(encodedBuffer);

    // Handle the case where we need to add a partial decompressed character to the output buffer.
    // This means we need to encode one extra character and add as many bytes as possible into the output buffer.
    // This is an expensive operation that is executed as a last resort, meaning we only do this when we were
    // not able to add any bytes to the output payload before.
    if (decodedCharBuffer.remaining() > 0 && encodedBuffer.remaining() > 0 && encodedBuffer.position() - off == 0) {
      encodePartialCharIntoByteBuffer(encodedBuffer);
    }

    //Return the number of bytes copied,
    // This is matches the actual position in the decoded buffer minus the initial offset
    return encodedBuffer.position() - off;
  }

  /**
   * Decode butes from the specified byteBuffer into the DecodedCharBuffer.
   *
   * @param buffer The target ByteBuffer
   */
  protected void decodeByteBufferIntoCharBuffer(ByteBuffer buffer) {
    int initialCharBufferPos = decodedCharBuffer.position();

    //Decode input buffer as characters.
    CoderResult decodeResult = decoder.decode(buffer, decodedCharBuffer, false);

    if (decodeResult.isError()) {
      LOG.error("Unable to decode payload from file as {}", sourceEncoding.getCharset().name());
      throw new CharacterDecodingException(decoder);
    }

    int finalCharBufferPos = decodedCharBuffer.position();
  }

  /**
   * Encode bytes from the decodedCharBuffer into the specified ByteBuffer.
   *
   * @param buffer The target ByteBuffer
   */
  protected void encodeCharBufferIntoByteBuffer(ByteBuffer buffer) {
    int initialCharBufferPos = decodedCharBuffer.position();

    //Decode as many chars as possible into the outgoing buffer.
    CoderResult encodeResult = encoder.encode(decodedCharBuffer, buffer, true);

    if (encodeResult.isError()) {
      LOG.error("Unable to encode decoded payload as UTF-8");
      throw new CharacterEncodingException(encoder);
    }

    int finalCharBufferPos = decodedCharBuffer.position();
  }

  /**
   * Encode a minimum of 1 additional character from the decoded char buffer into the output buffer.
   * <p>
   * Since the output is UTF-8, there can be up to 4 additional characters encoded in this temporary buffer.
   * <p>
   * Any remaining bytes that could not be added into the output stream are stored in the `partialOutputByteBuffer`
   * and consumed in the next invocation of the `decompress` method.
   *
   * @param outputBuffer the output buffer we'll use to store the partial bytes from a character.
   */
  protected void encodePartialCharIntoByteBuffer(ByteBuffer outputBuffer) {
    // UTF-8 characters can be up to 4 bytes long.
    // We start from 2 bytes as a 1-byte-long character would already fit in the encoded buffer.
    ByteBuffer additionalByteBuffer = ByteBuffer.allocate(4);

    encodeCharBufferIntoByteBuffer(additionalByteBuffer);

    //Set up additional char buffer for read in the next invocation of this method.
    additionalByteBuffer.flip();

    //Read as many bytes as possible from this additional char buffer.
    while (additionalByteBuffer.hasRemaining() && outputBuffer.hasRemaining()) {
      outputBuffer.put(additionalByteBuffer.get());
    }

    //Store remaining bytes in the Partial Byte Buffer
    partialOutputByteBuffer = additionalByteBuffer;
  }

  @Override
  public int getRemaining() {
    return inputByteBuffer.remaining();
  }

  @Override
  public void reset() {
    inputByteBuffer = ByteBuffer.allocate(0);
    decodedCharBuffer = CharBuffer.allocate(0);
    partialOutputByteBuffer = ByteBuffer.allocate(0);
  }

  @Override
  public void end() {
    this.reset();
  }

  /**
   * Runtime Character Decoding Exception in case we are unable to decode the supplied payload from the
   * specified charset.
   */
  public static class CharacterDecodingException extends RuntimeException {
    public CharacterDecodingException(CharsetDecoder decoder) {
      super(String.format("Unable to read from source as text encoded in '%s'", decoder.charset().name()));
    }
  }

  /**
   * Runtime Character Decoding Exception in case we are unable to encode the decoded payload into the
   * desired charset.
   */
  public static class CharacterEncodingException extends RuntimeException {
    public CharacterEncodingException(CharsetEncoder encoder) {
      super(String.format("Unable to encode byte payload as '%s'", encoder.charset().name()));
    }
  }
}
