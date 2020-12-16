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
import java.nio.charset.CharsetDecoder;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map;

/**
 * Enumeration containing all currently supported Fixed Length charsets.
 * <p>
 * This currently includes:
 * - UTF-32,
 * - ISO-8859 variants supported by Java
 * - Windows single-byte code pages supported by Java.
 */
public class FixedLengthCharset {
  public static final FixedLengthCharset UTF_32 = new FixedLengthCharset("UTF-32", Charset.forName("UTF-32"));
  private static final Map<String, FixedLengthCharset> ALLOWED_MULTIBYTE_CHARSETS = ImmutableMap.of(
    UTF_32.getName(), UTF_32
  );

  private final String name;
  private final Charset charset;

  FixedLengthCharset(String name, Charset charset) {
    this.name = name;
    this.charset = charset;
  }

  public String getName() {
    return name;
  }

  public Charset getCharset() {
    return charset;
  }

  /**
   * Find a FixedLengthCharset for a given encoding name. Throws a runtime exception if not found.
   *
   * @param name Charset name
   * @return FixedLengthCharset for the desired charset.
   */
  public static FixedLengthCharset forName(String name) {
    FixedLengthCharset charset = ALLOWED_MULTIBYTE_CHARSETS.get(name.toUpperCase());
    if (charset != null) {
      return charset;
    }
    Charset c = Charset.forName(name.toUpperCase());
    return new FixedLengthCharset(name, c);
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
      CharsetDecoder decoder = charset.newDecoder();
      return decoder.maxCharsPerByte() == 1 && decoder.averageCharsPerByte() == 1;
    } catch (UnsupportedCharsetException e) {
      return false;
    }
  }
}
