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

import com.google.common.collect.ImmutableMap;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map;

/**
 * Enumeration containing all currently supported Fixed Length charsets.
 * <p>
 * This currently includes:
 * - UTF-32
 * - Any single-byte charset encoding supported by Java, including the ISO-8859, Windows-125x and EBCDIC
 */
public class FixedLengthCharset {
  public static final FixedLengthCharset UTF_32 = new FixedLengthCharset("UTF-32", Charset.forName("UTF-32"), 4);
  private static final Map<String, FixedLengthCharset> ALLOWED_MULTIBYTE_CHARSETS = ImmutableMap.of(
    UTF_32.getName(), UTF_32
  );

  private final String name;
  private final Charset charset;
  private final int numBytesPerCharacter;

  FixedLengthCharset(String name, Charset charset, int numBytesPerCharacter) {
    this.name = name;
    this.charset = charset;
    this.numBytesPerCharacter = numBytesPerCharacter;
  }

  public String getName() {
    return name;
  }

  public Charset getCharset() {
    return charset;
  }

  public int getNumBytesPerCharacter() {
    return numBytesPerCharacter;
  }

  /**
   * Find a FixedLengthCharset for a given encoding name. Throws a runtime exception if not found.
   *
   * @param name Charset name
   * @return FixedLengthCharset for the desired charset.
   */
  public static FixedLengthCharset forName(String name) {
    if (!isValidEncoding(name)) {
      throw new IllegalArgumentException("Charset not supported: " + name);
    }

    FixedLengthCharset fixedLengthCharset = ALLOWED_MULTIBYTE_CHARSETS.get(name.toUpperCase());
    if (fixedLengthCharset != null) {
      return fixedLengthCharset;
    }
    Charset charset = Charset.forName(name.toUpperCase());
    return new FixedLengthCharset(name, charset, 1);
  }

  /**
   * Check if this file encoding is valid.
   *
   * @return boolean value specifying if this is a valid encoding or not.
   */
  public static boolean isValidEncoding(String name) {
    if (ALLOWED_MULTIBYTE_CHARSETS.containsKey(name.toUpperCase())) {
      return true;
    }
    try {
      Charset charset = Charset.forName(name.toUpperCase());
      CharsetEncoder encoder = charset.newEncoder();
      return encoder.maxBytesPerChar() == 1 && encoder.averageBytesPerChar() == 1;
    } catch (UnsupportedOperationException | UnsupportedCharsetException e) {
      return false;
    }
  }

  /**
   * Takes the first word of the file encoding string, as any further words are just charset descriptions.
   *
   * @param fileEncoding The file encoding parameter supplied by the user
   * @return the cleaned up file encoding name
   */
  public static String cleanFileEncodingName(String fileEncoding) {
    fileEncoding = fileEncoding.trim();

    return fileEncoding.split(" ")[0].toUpperCase();
  }
}
