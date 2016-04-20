/*
 * Copyright © 2016 Cask Data, Inc.
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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests for time parsing.
 */
public class TimeParserTest {

  @Test
  public void testDurationParse() {
    Assert.assertEquals(TimeUnit.MINUTES.toMillis(5), TimeParser.parseDuration("5m"));
    Assert.assertEquals(TimeUnit.SECONDS.toMillis(5), TimeParser.parseDuration("5s"));
    Assert.assertEquals(TimeUnit.HOURS.toMillis(5), TimeParser.parseDuration("5h"));
    Assert.assertEquals(TimeUnit.DAYS.toMillis(5), TimeParser.parseDuration("5d"));
    Assert.assertEquals(TimeUnit.DAYS.toMillis(5), TimeParser.parseDuration("+5d"));
    Assert.assertEquals(0 - TimeUnit.DAYS.toMillis(5), TimeParser.parseDuration("-5d"));
  }

  @Test
  public void testMathParse() {
    long runtime = 50L * 1000L;
    TimeParser timeParser = new TimeParser(runtime);
    Assert.assertEquals(runtime, timeParser.parseRuntime("runtime"));

    long expected = runtime + TimeUnit.DAYS.toMillis(1) - TimeUnit.HOURS.toMillis(2) +
      TimeUnit.MINUTES.toMillis(3) - TimeUnit.SECONDS.toMillis(4);
    Assert.assertEquals(expected, timeParser.parseRuntime(" runtime + 1d-2h+3m - 4s  "));

    Assert.assertEquals(TimeUnit.MINUTES.toMillis(5), timeParser.parseRuntime("5m"));
    Assert.assertEquals(TimeUnit.SECONDS.toMillis(5), timeParser.parseRuntime("5s"));
    Assert.assertEquals(TimeUnit.HOURS.toMillis(5), timeParser.parseRuntime("5h"));
    Assert.assertEquals(TimeUnit.DAYS.toMillis(5), timeParser.parseRuntime("5d"));
    Assert.assertEquals(TimeUnit.DAYS.toMillis(5), timeParser.parseRuntime("+5d"));
    Assert.assertEquals(0 - TimeUnit.DAYS.toMillis(5), timeParser.parseRuntime("-5d"));
  }
}
