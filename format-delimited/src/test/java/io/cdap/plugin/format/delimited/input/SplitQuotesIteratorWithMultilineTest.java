/*
 * Copyright © 2023 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class SplitQuotesIteratorWithMultilineTest {

  private SplitQuotesIterator splitQuotesIterator;

  private List<String> getListFromIterator(Iterator<String> iterator) {
    List<String> result = new ArrayList();
    while (iterator.hasNext()) {
      result.add(iterator.next());
    }
    return result;
  }

  @Test
  public void testStringShorterThanDelimiter() throws Exception {
    String test = "a";
    Assert.assertEquals(Arrays.asList(test.split(",,")),
                        getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, ",,", null, true)));

    Assert.assertEquals(Arrays.asList(test.split("aa")),
                        getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, "aa", null, true)));
  }

  @Test
  public void testStringWithConsecutiveDelimiter() throws Exception {
    String test = "aaa";
    Assert.assertEquals(Arrays.asList(test.split("a", -1)),
                        getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, "a", null, true)));


    Assert.assertEquals(Arrays.asList(test.split("aa", -1)),
                        getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, "aa", null, true)));

    test = "aaaaaaa";
    Assert.assertEquals(Arrays.asList(test.split("aa", -1)),
                        getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, "aa", null, true)));

    test = "aaaaaaaa";
    Assert.assertEquals(Arrays.asList(test.split("aa", -1)),
                        getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, "aa", null, true)));
  }

  @Test
  public void testSimpleSplit() throws Exception {
    String test = "a,b,c,d,e";
    Assert.assertEquals(Arrays.asList(test.split(",")),
                        getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, ",", null, true)));

    test = "a1,b1,c1,d1,e1";
    Assert.assertEquals(Arrays.asList(test.split(",")),
                        getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, ",", null, true)));

    test = "1###sam###a@b.com###male";
    Assert.assertEquals(Arrays.asList(test.split("###")),
                        getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, "###", null, true)));

    test = "a1,,,b1,,,c1,,,d1,,,e1";
    Assert.assertEquals(Arrays.asList(test.split(",")),
                        getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, ",", null, true)));
  }

  @Test
  public void testValidQuotesWithTrimedQuotes() throws Exception {
    String test = "a,\"b,c\"";
    List<String> expected = ImmutableList.of("a", "b,c");
    Assert.assertEquals(expected, getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, ",",
                                                                                                    null, true)));

    test = "a,\"aaaaa\"aaaab\"aaaac\",c";
    expected = ImmutableList.of("a", "aaaaaaaaabaaaac", "c");
    Assert.assertEquals(expected, getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, ",",
                                                                                                    null, true)));

    test = "a,\"b,c\",\"d,e,f\"";
    expected = ImmutableList.of("a", "b,c", "d,e,f");
    Assert.assertEquals(expected, getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, ",",
                                                                                                    null, true)));

    test = "a###\"b###c\"###\"d###e###f\"";
    expected = ImmutableList.of("a", "b###c", "d###e###f");
    Assert.assertEquals(expected, getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, "###"
      , null, true)));
  }

  @Test
  public void testBadQuotes() throws Exception {
    RecordReader recordReader = getRecordReader(null);
    String test = "Value1,value2.1 value2.2\"value2.2.1,value2.3\",val\"ue3,value4";
    IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () -> {
      getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, ",", recordReader, true));
    });
    Assert.assertTrue(exception.getMessage().contains("Found a line with an unenclosed quote. Ensure that all"
                                                        + " values are properly quoted, or disable quoted values."));
  }

  @Test
  public void testNextLineWithMultilineSupport() {
    RecordReader recordReader = getRecordReader("data in next line\"");
    String test = "Value1,\"value2.1.1,value2.1.2\",\"data in first line ";
    List<String> expected = ImmutableList.of("Value1", "value2.1.1,value2.1.2",
                                             "data in first line \ndata in next line");
    Assert.assertEquals(expected, getListFromIterator(splitQuotesIterator = new SplitQuotesIterator(test, ",",
                                                                                                    recordReader,
                                                                                                    true)));


  }


  public RecordReader getRecordReader(String nextValue) {
    return new RecordReader() {
      @Override
      public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException,
        InterruptedException {
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return nextValue != null;
      }

      @Override
      public Object getCurrentKey() throws IOException, InterruptedException {
        return null;
      }

      @Override
      public Object getCurrentValue() throws IOException, InterruptedException {
        return nextValue;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return 0;
      }

      @Override
      public void close() throws IOException {

      }
    };
  }
}
