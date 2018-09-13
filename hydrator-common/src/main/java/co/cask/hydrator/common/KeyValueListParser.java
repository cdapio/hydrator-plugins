/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.hydrator.common;

import co.cask.cdap.api.dataset.lib.KeyValue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Parses a list of key value pairs.
 */
public class KeyValueListParser {
  private final Pattern pairDelimiter;
  private final Pattern keyValDelimiter;
  public static final KeyValueListParser DEFAULT = new KeyValueListParser(",", ":");

  /**
   * Create a parser that uses the given regexes to parse a list of key value pairs.
   *
   * @param pairDelimiterRegex the delimiter between key value pairs.
   * @param keyValDelimiterRegex the delimiter between a key and value.
   */
  public KeyValueListParser(String pairDelimiterRegex, String keyValDelimiterRegex) {
    pairDelimiter = Pattern.compile(pairDelimiterRegex);
    keyValDelimiter = Pattern.compile(keyValDelimiterRegex);
  }

  /**
   * Parses the given list of key value pairs.
   *
   * @param kvList the string to parse
   * @return an iterable of key values
   */
  public Iterable<KeyValue<String, String>> parse(String kvList) {
    return new KeyValueIterable(kvList);
  }

  private class KeyValueIterable implements Iterable<KeyValue<String, String>> {
    private final String kvList;

    private KeyValueIterable(String kvList) {
      this.kvList = kvList;
    }

    @Override
    public Iterator<KeyValue<String, String>> iterator() {
      return new KeyValueIterator(kvList);
    }
  }

  private class KeyValueIterator implements Iterator<KeyValue<String, String>> {
    private final Iterator<String> pairIter;

    private KeyValueIterator(String kvList) {
      pairIter = Arrays.stream(pairDelimiter.split(kvList))
        .map(String::trim)
        .collect(Collectors.toList())
        .iterator();
    }

    @Override
    public boolean hasNext() {
      return pairIter.hasNext();
    }

    @Override
    public KeyValue<String, String> next() {
      String pair = pairIter.next();
      Iterator<String> keyValIter = Arrays.stream(keyValDelimiter.split(pair))
        .map(String::trim)
        .collect(Collectors.toList())
        .iterator();
      String key = keyValIter.next();
      if (!keyValIter.hasNext()) {
        throw new IllegalArgumentException(String.format("Invalid syntax for key-value pair in list: %s. " +
                                                           "It is expected to be a string separated " +
                                                           "by exactly one %s", pair, keyValDelimiter));
      }
      String val = keyValIter.next();
      if (keyValIter.hasNext()) {
        throw new IllegalArgumentException(String.format("Invalid syntax for key-value pair in list: %s. " +
                                                           "It is expected to be a string separated " +
                                                           "by exactly one %s", pair, keyValDelimiter));
      }
      return new KeyValue<>(key, val);
    }

    @Override
    public void remove() {
      //no-op
    }
  }
}
