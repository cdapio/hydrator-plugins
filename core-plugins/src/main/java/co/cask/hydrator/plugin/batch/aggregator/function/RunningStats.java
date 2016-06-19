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

/**
 * Computes Mean, Variance, Standard Deviation, Skewness and Kurtosis in single pass.
 * Uses Knuth and Welford for computing Standard Deviation in one pass through data.
 * http://www.johndcook.com/blog/skewness_kurtosis/
 */
public final class RunningStats  {
  private long entries = 0L;
  private double mean1, mean2, mean3, mean4 = 0d;

  /**
   * Pushes a number into machinary that computes a lot of statistics.
   * @param x number to be added to computing statistics.
   */
  public void push(double x) {
    double delta, deltaN, deltaN2, term1;

    long n1 = entries;
    entries++;
    delta = x - mean1;
    deltaN = delta / entries;
    deltaN2 = deltaN * deltaN;
    term1 = delta * deltaN * n1;
    mean1 += deltaN;
    mean4 += term1 * deltaN2 * (entries * entries - 3 * entries + 3) + 6 * deltaN2 * mean2 - 4 * deltaN * mean3;
    mean3 += term1 * deltaN * (entries - 2) - 3 * deltaN * mean2;
    mean2 += term1;
  }

  /**
   * @return Mean of all the numbers.
   */
  public double mean() {
    return mean1;
  }

  /**
   * @return Variance of all numbers.
   */
  public double variance() {
    return mean2 / (entries - 1.0d);
  }

  /**
   * @return Standard Deviation of all numbers.
   */
  public double stddev() {
    return Math.sqrt(variance());
  }

  /**
   * @return Skewness of all numbers.
   */
  public double skewness() {
    return Math.sqrt((double) entries) * mean3 / Math.pow(mean2, 1.5d);
  }

  /**
   * @return Kurtosis of all numbers.
   */
  public double kurtosis() {
    return (double) entries * mean4 / (mean2 * mean2) - 3.0;
  }
}
