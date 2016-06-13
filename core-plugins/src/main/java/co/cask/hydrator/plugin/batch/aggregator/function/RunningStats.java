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

package co.cask.hydrator.plugin.batch.aggregator.function;

public final class RunningStats  {
  private long entries = 0l;
  private double oldM = 0d;
  private double newM = 0d;
  private double oldS = 0d;
  private double newS = 0d;

  public void push(double x) {
    entries = entries + 1;
    // See Knuth TAOCP vol 2, 3rd edition, page 232
    if (entries == 1) {
      oldM = newM = x;
      oldS = 0.0;
    }  else  {
      newM = oldM + (x - oldM) / entries;
      newS = oldS + (x - oldM) * (x - newM);
      // set up for next iteration
      oldM = newM;
      oldS = newS;
    }
  }

  public long values() {
    return entries;
  }

  public double mean() {
    return (entries > 0) ? newM : 0.0;
  }

  public double variance() {
    return ( (entries > 1) ? newS /(entries - 1) : 0.0 );
  }

  public double stddev() {
    return Math.sqrt(variance());
  }
}
