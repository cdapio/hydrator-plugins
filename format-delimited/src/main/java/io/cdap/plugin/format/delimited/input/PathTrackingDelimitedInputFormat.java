/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.SchemaValidator;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Delimited text format that tracks which file each record was read from.
 */
public class PathTrackingDelimitedInputFormat extends PathTrackingInputFormat {
  static final String DELIMITER = "delimiter";
  static final String ENABLE_QUOTES_VALUE = "enable_quotes_value";
  static final String SKIP_HEADER = "skip_header";

  private static final String QUOTE = "\"";
  private static final char QUOTE_CHAR = '\"';

  /**
   * Split the delimited string based on the delimiter. The delimiter should not contain any quotes.
   * The method will behave like this: 1. if there is no quote, it will behave same as {@link
   * String#split(String)} 2. if there are quotes in the string, the method will find pairs of
   * quotes, content within each pair of quotes will not get splitted even if there is delimiter in
   * that. For example, if string is a."b.c"."d.e.f" and delimiter is '.', it will get split into
   * [a, b.c, d.e.f]. if string is "val1.val2", then it will not get splitted since the '.' is
   * within pair of quotes. if string is "val1.val2"", it's invalid since the last quote is not
   * closed.
   *
   * @param delimitedString the string to split
   * @param delimiter the separtor
   * @return a list of splits of the original string
   */
  @VisibleForTesting
  static List<String> splitQuotesString(String delimitedString, String delimiter) {
    // use a tree map so we can easily look up if a delimiter is within a pair of quotes
    // the map has key of index and value to the index of paired quote. The map key is the index of
    // the starting and the ending quote and the value is the index of the paired quote. An
    // un-enclosed quote will trigger the exception and fail the pipeline. Example:
    // pos: 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
    //      " | a | , | b | " | , | " | c | , | d | "  |
    // key-value pairs in the map:
    // 0 -> 4
    // 4 -> 0
    // 6 -> 10
    // 10 -> 6
    TreeMap<Integer, Integer> quotesPos = new TreeMap<>();
    int firstQuotePos = -1;
    for (int i = 0; i < delimitedString.length(); i++) {
      if (delimitedString.charAt(i) != QUOTE_CHAR) {
        continue;
      }

      if (firstQuotePos == -1) {
        firstQuotePos = i;
      } else {
        quotesPos.put(i, firstQuotePos);
        quotesPos.put(firstQuotePos, i);
        firstQuotePos = -1;
      }
    }

    // throws exception if there is unclosed quote
    if (firstQuotePos != -1) {
      throw new RuntimeException("There is unclosed quote in the value: " + delimitedString);
    }

    List<String> result = new ArrayList<>();
    StringBuilder split = new StringBuilder();
    for (int i = 0; i < delimitedString.length(); i++) {
      // if the length is not enough for the delimiter, just add it to split
      if (i + delimiter.length() > delimitedString.length()) {
        split.append(delimitedString.charAt(i));
      } else {
        // not a delimiter
        if (!delimitedString.startsWith(delimiter, i)) {
          split.append(delimitedString.charAt(i));
          continue;
        }

        // find a delimiter
        // find the prev quote and next quote index
        Integer prevQuotePos = quotesPos.floorKey(i);
        Integer nextQuotePos = quotesPos.ceilingKey(i);
        // if the they exist and they form a pair, we will not split, just add the current character
        // to split
        if (prevQuotePos != null
            && nextQuotePos != null
            && quotesPos.get(prevQuotePos).equals(nextQuotePos)) {
          split.append(delimitedString, i, nextQuotePos + 1);
          i = nextQuotePos;
        } else {
          // else we need to add the finding split to result, and increment the index, reset the
          // split
          result.add(trimQuotes(split.toString()));
          i = i + delimiter.length() - 1;
          split = new StringBuilder();
        }
      }
    }

    // add what is remaining to the result
    result.add(trimQuotes(split.toString()));
    return result;
  }

  /** Trim the quotes if and only if the start and end of the string are quotes */
  private static String trimQuotes(String string) {
    if (string.length() > 1
        && string.charAt(0) == QUOTE_CHAR
        && string.charAt(string.length() - 1) == '\"') {
      return string.replaceAll("^\"|\"$", "");
    }
    return string;
  }

  @Override
  protected RecordReader<NullWritable, StructuredRecord.Builder> createRecordReader(FileSplit split,
                                                                                    TaskAttemptContext context,
                                                                                    @Nullable String pathField,
                                                                                    @Nullable Schema schema) {

    RecordReader<LongWritable, Text> delegate = getDefaultRecordReaderDelegate(split, context);
    String delimiter = context.getConfiguration().get(DELIMITER);
    boolean skipHeader = context.getConfiguration().getBoolean(SKIP_HEADER, false);
    boolean enableQuotesValue = context.getConfiguration().getBoolean(ENABLE_QUOTES_VALUE, false);

    return new RecordReader<NullWritable, StructuredRecord.Builder>() {

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        delegate.initialize(split, context);
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (delegate.nextKeyValue()) {
          // skip to next if the current record is header
          if (skipHeader && delegate.getCurrentKey().get() == 0L) {
            return delegate.nextKeyValue();
          }
          return true;
        }
        return false;
      }

      @Override
      public NullWritable getCurrentKey() {
        return NullWritable.get();
      }

      @Override
      public StructuredRecord.Builder getCurrentValue() throws IOException, InterruptedException {
        String delimitedString = delegate.getCurrentValue().toString();

        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        Iterator<Schema.Field> fields = schema.getFields().iterator();
        List<String> splits = new ArrayList<>();
        if (!enableQuotesValue) {
          Iterable<String> iter = Splitter.on(delimiter).split(delimitedString);
          iter.forEach(splits::add);
        } else {
          splits = splitQuotesString(delimitedString, delimiter);
        }

        for (String part : splits) {
          if (!fields.hasNext()) {
            int numDataFields = splits.size();
            int numSchemaFields = schema.getFields().size();
            String message =
                String.format(
                    "Found a row with %d fields when the schema only contains %d field%s.",
                    numDataFields, numSchemaFields, numSchemaFields == 1 ? "" : "s");
            // special error handling for the case when the user most likely set the schema to
            // delimited
            // when they meant to use 'text'.
            Schema.Field bodyField = schema.getField("body");
            if (bodyField != null) {
              Schema bodySchema = bodyField.getSchema();
              bodySchema = bodySchema.isNullable() ? bodySchema.getNonNullable() : bodySchema;
              if (bodySchema.getType() == Schema.Type.STRING) {
                throw new IOException(message + " Did you mean to use the 'text' format?");
              }
            }
            if (!enableQuotesValue && delimitedString.contains(QUOTE)) {
              message += " Check if quoted values should be allowed.";
            }
            throw new IOException(
                message + " Check that the schema contains the right number of fields.");
          }

          Schema.Field nextField = fields.next();
          if (part.isEmpty()) {
            builder.set(nextField.getName(), null);
          } else {
            String fieldName = nextField.getName();
            // Ensure if date time field, value is in correct format
            SchemaValidator.validateDateTimeField(nextField.getSchema(), fieldName, part);
            builder.convertAndSet(fieldName, part);
          }
        }
        return builder;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return delegate.getProgress();
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    };
  }
}
