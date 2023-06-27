/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.format.delimited.input;

import com.google.common.collect.AbstractIterator;
import org.apache.hadoop.mapreduce.RecordReader;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Iterator that provides the splits in the delimited string based on the delimiter. The delimiter
 * should not contain any quotes. The splitor will behave like this: 1. if there is no quote, it
 * will behave same as {@link String#split(String)} 2. if there are quotes in the string, the method
 * will find pairs of quotes, content within each pair of quotes will not get splitted even if there
 * is delimiter in that. For example, if string is a."b.c"."d.e.f" and delimiter is '.', it will get
 * split into [a, b.c, d.e.f]. if string is "val1.val2", then it will not get splitted since the '.'
 * is within pair of quotes. If the delimited string contains odd number of quotes, which mean the
 * quotes are not closed, an exception will be thrown. The quote within the value will always be
 * trimed.
 */
public class SplitQuotesIterator extends AbstractIterator<String> {
  private static final char QUOTE_CHAR = '\"';
  private static final String LINE_SEPARATOR = "\n";
  private final String delimiter;
  private final RecordReader recordReader;
  private final boolean enableMultilineSupport;
  private String delimitedString;
  private int index;
  private boolean endingWithDelimiter = false;

  public SplitQuotesIterator(String delimitedString, String delimiter, @Nullable RecordReader recordReader,
                             boolean enableMultilineSupport) {
    this.delimitedString = delimitedString;
    this.delimiter = delimiter;
    this.recordReader = recordReader;
    this.enableMultilineSupport = enableMultilineSupport;
    index = 0;
  }

  @Override
  protected String computeNext() {
    // Corner case when the delimiter is in the end of the row
    if (endingWithDelimiter) {
      endingWithDelimiter = false;
      return "";
    }

    if (index == delimitedString.length()) {
      return endOfData();
    }

    boolean isWithinQuotes = false;
    StringBuilder split = new StringBuilder();
    while (index < delimitedString.length()) {
      char cur = delimitedString.charAt(index);
      if (cur == QUOTE_CHAR) {
        isWithinQuotes = !isWithinQuotes;
        index++;
        appendNextLineIfNeeded(split, isWithinQuotes);
        continue;
      }

      // if the length is not enough for the delimiter or it's not a delimiter, just add it to split
      if (index + delimiter.length() > delimitedString.length() ||
        !delimitedString.startsWith(delimiter, index)) {
        split.append(cur);
        index++;
        appendNextLineIfNeeded(split, isWithinQuotes);
        continue;
      }

      // find delimiter not within quotes
      if (!isWithinQuotes) {
        index += delimiter.length();
        if (index == delimitedString.length()) {
          endingWithDelimiter = true;
        }
        return split.toString();
      }

      // delimiter within quotes
      split.append(cur);
      index++;
      appendNextLineIfNeeded(split, isWithinQuotes);
    }

    if (isWithinQuotes) {
      throw new IllegalArgumentException(
        "Found a line with an unenclosed quote. Ensure that all values are properly"
          + " quoted, or disable quoted values.");
    }

    return split.toString();
  }


  /**
   * This method will fetch the next line from Record Reader if split is still within quotes which means data is also
   * there in next line.
   *
   * @param isWithinQuotes whether current split value is within quotes or not
   */
  private void appendNextLineIfNeeded(StringBuilder split, boolean isWithinQuotes) {
    try {
      if (enableMultilineSupport && index == delimitedString.length() && isWithinQuotes) {
        if (!recordReader.nextKeyValue()) {
          throw new IllegalArgumentException(
            "Found a line with an unenclosed quote. Ensure that all values are properly"
              + " quoted, or disable quoted values.");
        } else {
          split.append(LINE_SEPARATOR);
          delimitedString = recordReader.getCurrentValue().toString();
          index = 0;
        }
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
