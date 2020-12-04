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

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FixedLengthCharsetTransformingDecompressorTest {

  FixedLengthCharsetTransformingDecompressor decompressor;

  @Test
  public void testUtf32Input() throws IOException {
    testDecode("This is 漢字áéíóú@ string that will need to be converted into UTF-8",
               FixedLengthCharset.UTF_32);
  }

  @Test
  public void testISO88591Input() throws IOException {
    testDecode("This is áéíóú@ string that will need to be converted into UTF-8",
               FixedLengthCharset.ISO_8859_1);
  }

  @Test
  public void testISO88592Input() throws IOException {
    testDecode("This is ŕęďţ@ string that will need to be converted into UTF-8",
               FixedLengthCharset.ISO_8859_2);
  }

  @Test
  public void testISO88593Input() throws IOException {
    testDecode("This is ĜŝħĦ@ string that will need to be converted into UTF-8",
               FixedLengthCharset.ISO_8859_3);
  }

  @Test
  public void testISO88594Input() throws IOException {
    testDecode("This is ŖąÆŦĸ@ string that will need to be converted into UTF-8",
               FixedLengthCharset.ISO_8859_4);
  }

  @Test
  public void testISO88595Input() throws IOException {
    testDecode("This is ДЩЊЮѓ@ string that will need to be converted into UTF-8",
               FixedLengthCharset.ISO_8859_5);
  }

  @Test
  public void testISO88596Input() throws IOException {
    testDecode("This is سشغقئ@ string that will need to be converted into UTF-8",
               FixedLengthCharset.ISO_8859_6);
  }

  @Test
  public void testWindows1252Bytes() throws IOException {
    testDecode("This is áéíóú@ string that will need to be converted into UTF-8",
               FixedLengthCharset.WINDOWS_1252);
  }

  public void testDecode(String testString, FixedLengthCharset fixedLengthCharset) throws IOException {
    decompressor = new FixedLengthCharsetTransformingDecompressor(fixedLengthCharset);
    ByteArrayOutputStream decompressed = new ByteArrayOutputStream();

    byte[] sourceBytes = testString.getBytes(fixedLengthCharset.getCharset());
    byte[] utf8Bytes = testString.getBytes(StandardCharsets.UTF_8);

    //Set the input stream 3 bytes at a time.
    for (int i = 0; i < sourceBytes.length; i += 3) {
      decompressor.setInput(sourceBytes, i, Math.min(3, sourceBytes.length - i));

      //Read from the decompressed stream one byte at a time.
      while (!decompressor.finished()) {
        byte[] buf = new byte[1];
        int numReadBytes = decompressor.decompress(buf, 0, 1);
        decompressed.write(buf, 0, numReadBytes);
      }
    }

    byte[] decompressedBytes = decompressed.toByteArray();
    Assert.assertArrayEquals(utf8Bytes, decompressedBytes);
  }
}
