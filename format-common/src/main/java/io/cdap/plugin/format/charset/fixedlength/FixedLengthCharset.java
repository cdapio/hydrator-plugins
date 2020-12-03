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
 * Enumeration containing all supported Fixed Length charsets.
 */
public enum FixedLengthCharset {
  UTF_32("UTF-32", Charset.forName("UTF-32"), 4),
  ISO_8859_1("ISO-8859-1", StandardCharsets.ISO_8859_1, 1),
  WINDOWS_1252("Windows-1252", Charset.forName("windows-1252"), 1);

  private final String name;
  private final Charset charset;
  private final int charLength;

  FixedLengthCharset(String name, Charset charset, int charLength) {
    this.name = name;
    this.charset = charset;
    this.charLength = charLength;
  }

  public String getName() {
    return name;
  }

  public Charset getCharset() {
    return charset;
  }

  public int getCharLength() {
    return charLength;
  }

  /**
   * Find a FixedLengthCharset for a given encoding name. Throws a runtime exception if not found.
   * @param name Charset name
   * @return FixedLengthCharset for the desired charset.
   */
  public static FixedLengthCharset forName(String name) {
    for (FixedLengthCharset c : FixedLengthCharset.values()) {
      if (name.equalsIgnoreCase(c.getName())) {
        return c;
      }
    }

    throw new RuntimeException("Cannot find FixedLengthCharset with name: " + name);
  }

  /**
   * Set containing the names of all character encodings.
   *
   * @return A set containing all character encoding names.
   */
  public static Set<String> getValidEncodings() {
    Set<String> set = new HashSet<>();
    for (FixedLengthCharset c : FixedLengthCharset.values()) {
      set.add(c.getName().toLowerCase());
    }
    return Collections.unmodifiableSet(set);
  }
}
