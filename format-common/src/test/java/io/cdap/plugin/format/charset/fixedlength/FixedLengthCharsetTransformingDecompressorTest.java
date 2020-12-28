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
    testDecode("áéíóú This is a string that will need to be converted into UTF-8",
               FixedLengthCharset.UTF_32);
  }

  @Test
  public void testISO88591Input() throws IOException {
    testDecode("This is áéíóú@ string that will need to be converted into UTF-8",
               FixedLengthCharset.forName("ISO-8859-1"));
  }

  @Test
  public void testISO88592Input() throws IOException {
    testDecode("This is ŕęďţ@ string that will need to be converted into UTF-8",
               FixedLengthCharset.forName("ISO-8859-2"));
  }

  @Test
  public void testISO88593Input() throws IOException {
    testDecode("This is ĜŝħĦ@ string that will need to be converted into UTF-8",
               FixedLengthCharset.forName("ISO-8859-3"));
  }

  @Test
  public void testISO88594Input() throws IOException {
    testDecode("This is ŖąÆŦĸ@ string that will need to be converted into UTF-8",
               FixedLengthCharset.forName("ISO-8859-4"));
  }

  @Test
  public void testISO88595Input() throws IOException {
    testDecode("This is ДЩЊЮѓ@ string that will need to be converted into UTF-8",
               FixedLengthCharset.forName("ISO-8859-5"));
  }

  @Test
  public void testISO88596Input() throws IOException {
    testDecode("This is سشغقئ@ string that will need to be converted into UTF-8",
               FixedLengthCharset.forName("ISO-8859-6"));
  }

  @Test
  public void testWindows1252Bytes() throws IOException {
    testDecode("This is áéíóú@ string that will need to be converted into UTF-8",
               FixedLengthCharset.forName("Windows-1252"));
  }

  @Test
  public void testIBM1047Bytes() throws IOException {
    testDecode("This is a string that will need to be converted into UTF-8",
               FixedLengthCharset.forName("IBM1047"));
  }

  public void testDecode(String testString, FixedLengthCharset fixedLengthCharset) throws IOException {
    decompressor = new FixedLengthCharsetTransformingDecompressor(fixedLengthCharset);
    ByteArrayOutputStream decompressed = new ByteArrayOutputStream();

    byte[] sourceBytes = testString.getBytes(fixedLengthCharset.getCharset());
    byte[] utf8Bytes = testString.getBytes(StandardCharsets.UTF_8);

    //Consume input and output at different speeds to ensure the decompressor works as expected.
    for (int inputBlockSize = 1; inputBlockSize <= 16; inputBlockSize++) {
      for (int outputBlockSize = 1; outputBlockSize <= 16; outputBlockSize++) {

        //Set input and decompress.
        for (int i = 0; i < sourceBytes.length; i += inputBlockSize) {
          decompressor.setInput(sourceBytes, i, Math.min(inputBlockSize, sourceBytes.length - i));

          //Read from the decompressed stream one byte at a time.
          while (!decompressor.finished()) {
            byte[] buf = new byte[outputBlockSize];
            int numReadBytes = decompressor.decompress(buf, 0, outputBlockSize);
            decompressed.write(buf, 0, numReadBytes);
          }
        }

        //Ensure decompressed output matches expected payload.
        byte[] decompressedBytes = decompressed.toByteArray();
        Assert.assertArrayEquals(utf8Bytes, decompressedBytes);

        decompressed.reset();
        decompressor.reset();

      }
    }
  }
}
