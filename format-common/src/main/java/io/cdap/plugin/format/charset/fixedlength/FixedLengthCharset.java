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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Enumeration containing all currently supported Fixed Length charsets.
 * <p>
 * This currently includes:
 * - UTF-32,
 * - ISO-8859 variants supported by Java
 * - Windows single-byte code pages supported by Java.
 */
public enum FixedLengthCharset {
  UTF_32("UTF-32", Charset.forName("UTF-32"), 4),
  ISO_8859_1("ISO-8859-1", StandardCharsets.ISO_8859_1, 1),
  ISO_8859_2("ISO-8859-2", Charset.forName("ISO-8859-2"), 1),
  ISO_8859_3("ISO-8859-3", Charset.forName("ISO-8859-3"), 1),
  ISO_8859_4("ISO-8859-4", Charset.forName("ISO-8859-4"), 1),
  ISO_8859_5("ISO-8859-5", Charset.forName("ISO-8859-5"), 1),
  ISO_8859_6("ISO-8859-6", Charset.forName("ISO-8859-6"), 1),
  ISO_8859_7("ISO-8859-7", Charset.forName("ISO-8859-7"), 1),
  ISO_8859_8("ISO-8859-8", Charset.forName("ISO-8859-8"), 1),
  ISO_8859_9("ISO-8859-9", Charset.forName("ISO-8859-9"), 1),
  ISO_8859_11("ISO-8859-11", Charset.forName("ISO-8859-11"), 1),
  ISO_8859_13("ISO-8859-13", Charset.forName("ISO-8859-13"), 1),
  ISO_8859_15("ISO-8859-15", Charset.forName("ISO-8859-15"), 1),
  WINDOWS_1250("Windows-1250", Charset.forName("windows-1250"), 1),
  WINDOWS_1251("Windows-1251", Charset.forName("windows-1251"), 1),
  WINDOWS_1252("Windows-1252", Charset.forName("windows-1252"), 1),
  WINDOWS_1253("Windows-1253", Charset.forName("windows-1253"), 1),
  WINDOWS_1254("Windows-1254", Charset.forName("windows-1254"), 1),
  WINDOWS_1255("Windows-1255", Charset.forName("windows-1255"), 1),
  WINDOWS_1256("Windows-1256", Charset.forName("windows-1256"), 1),
  WINDOWS_1257("Windows-1257", Charset.forName("windows-1257"), 1),
  WINDOWS_1258("Windows-1258", Charset.forName("windows-1258"), 1);

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
    for (FixedLengthCharset c : FixedLengthCharset.values()) {
      if (name.equalsIgnoreCase(c.getName())) {
        return c;
      }
    }

    throw new IllegalArgumentException("Cannot find FixedLengthCharset for the supplied charset name: '" + name +"'");
  }

  /**
   * Check if this file encoding is valid.
   *
   * @return boolean value specifying if this is a valid encoding or not.
   */
  public static boolean isValidEncoding(String name) {
    for (FixedLengthCharset c : FixedLengthCharset.values()) {
      if (name.equalsIgnoreCase(c.getName())) {
        return true;
      }
    }

    return false;
  }
}
