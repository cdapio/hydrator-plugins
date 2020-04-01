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

package io.cdap.plugin.format.delimited.input;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DelimitedStringTest {

  @Test
  public void testStringShorterThanDelimiter() throws Exception {
    String test = "a";
    Assert.assertEquals(Arrays.asList(test.split(",,")),
                        PathTrackingDelimitedInputFormat.splitQuotesString(test, ",,"));
    Assert.assertEquals(Arrays.asList(test.split("aa")),
                        PathTrackingDelimitedInputFormat.splitQuotesString(test, "aa"));

    test = "\"a";
    Assert.assertEquals(Arrays.asList(test.split("aaa")),
                        PathTrackingDelimitedInputFormat.splitQuotesString(test, "aaa"));
    Assert.assertEquals(Arrays.asList(test.split(",,,")),
                        PathTrackingDelimitedInputFormat.splitQuotesString(test, ",,,"));
  }

  @Test
  public void testStringWithConsecutiveDelimiter() throws Exception {
    String test = "aaa";
    Assert.assertEquals(Arrays.asList(test.split("a")),
                        PathTrackingDelimitedInputFormat.splitQuotesString(test, "a"));
    Assert.assertEquals(Arrays.asList(test.split("aa")),
                        PathTrackingDelimitedInputFormat.splitQuotesString(test, "aa"));

    test = "aaaaaaa";
    Assert.assertEquals(Arrays.asList(test.split("aa")),
                        PathTrackingDelimitedInputFormat.splitQuotesString(test, "aa"));
    test = "aaaaaaaa";
    Assert.assertEquals(Arrays.asList(test.split("aa")),
                        PathTrackingDelimitedInputFormat.splitQuotesString(test, "aa"));
  }

  @Test
  public void testSimpleSplit() throws Exception {
    String test = "a,b,c,d,e";
    Assert.assertEquals(Arrays.asList(test.split(",")),
                        PathTrackingDelimitedInputFormat.splitQuotesString(test, ","));

    test = "a1,b1,c1,d1,e1";
    Assert.assertEquals(Arrays.asList(test.split(",")),
                        PathTrackingDelimitedInputFormat.splitQuotesString(test, ","));

    test = "1###sam###a@b.com###male";
    Assert.assertEquals(Arrays.asList(test.split("###")),
                        PathTrackingDelimitedInputFormat.splitQuotesString(test, "###"));

    test = "a1,,,b1,,,c1,,,d1,,,e1";
    Assert.assertEquals(Arrays.asList(test.split(",")),
                        PathTrackingDelimitedInputFormat.splitQuotesString(test, ","));
  }

  @Test
  public void testValidQuotesWithTrimedQuotes() {
    String test = "a,\"b,c\"";
    List<String> expected = ImmutableList.of("a", "b,c");
    Assert.assertEquals(expected, PathTrackingDelimitedInputFormat.splitQuotesString(test, ","));

    test = "a,\"aaaaa\"aaaab\"aaaac\",c";
    expected = ImmutableList.of("a", "aaaaa\"aaaab\"aaaac", "c");
    Assert.assertEquals(expected, PathTrackingDelimitedInputFormat.splitQuotesString(test, ","));

    test = "a,\"b,c\",\"d,e,f\"";
    expected = ImmutableList.of("a", "b,c", "d,e,f");
    Assert.assertEquals(expected, PathTrackingDelimitedInputFormat.splitQuotesString(test, ","));

    test = "a###\"b###c\"###\"d###e###f\"";
    expected = ImmutableList.of("a", "b###c", "d###e###f");
    Assert.assertEquals(expected, PathTrackingDelimitedInputFormat.splitQuotesString(test, "###"));
  }

  @Test
  public void testBadQuotes() {
    String test = "Value1,value2.1 value2.2\"value2.2.1,value2.3\",val\"ue3,value4";
    List<String> expected = ImmutableList.of("Value1", "value2.1 value2.2\"value2.2.1,value2.3\"",
                                             "val\"ue3", "value4");
    Assert.assertEquals(expected, PathTrackingDelimitedInputFormat.splitQuotesString(test, ","));

    test = "val1\",\"val2";
    expected = ImmutableList.of("val1\",\"val2");
    Assert.assertEquals(expected, PathTrackingDelimitedInputFormat.splitQuotesString(test, ","));

    test = "val1\",\"val2\"";
    expected = ImmutableList.of("val1\",\"val2\"");
    Assert.assertEquals(expected, PathTrackingDelimitedInputFormat.splitQuotesString(test, ","));
  }
}
