/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.aggregator.function;

import org.junit.Assert;
import org.junit.Test;

public class RunningStatsTest {

  @Test
  public void testMean() {
    RunningStats runningStats = new RunningStats();
    runningStats.push(0.0d);
    runningStats.push(40.0d);
    runningStats.push(100.0d);
    runningStats.push(100.0d);
    Assert.assertEquals(60.0d, runningStats.mean(), 0.001);
  }

  @Test
  public void testMeanWithOneEntry() {
    RunningStats runningStats = new RunningStats();
    runningStats.push(5.2d);
    Assert.assertEquals(5.2d, runningStats.mean(), 0.001);
  }

  @Test
  public void testMeanWithNoEntry() {
    RunningStats runningStats = new RunningStats();
    Assert.assertEquals(0.0d, runningStats.mean(), 0.001);
  }

  @Test
  public void testVariance() {
    RunningStats runningStats = new RunningStats();
    runningStats.push(5);
    runningStats.push(6);
    runningStats.push(10);
    runningStats.push(14);
    runningStats.push(15);

    Assert.assertEquals(4.04969, runningStats.stddev(), 0.001);
  }

  @Test
  public void testVarianceWithOneEntry() {
    RunningStats runningStats = new RunningStats();
    runningStats.push(5);
    Assert.assertEquals(0.0d, runningStats.stddev(), 0.001);
  }

  @Test
  public void testVarianceWithNoEntry() {
    RunningStats runningStats = new RunningStats();
    Assert.assertEquals(0.0d, runningStats.stddev(), 0.001);
  }
}
