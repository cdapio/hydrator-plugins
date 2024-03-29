/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Scanner;
import java.util.function.Supplier;

/**
 *
 */
public class NumberTest extends AggregateFunctionTest {

  protected void testFunction(AggregateFunction func, Schema schema, AggregateFunction otherFunc, Number expected,
                              Number... inputs) {
    Number actual = (Number) getAggregate(func, schema, "x", Arrays.asList(inputs), otherFunc);
    if (expected instanceof Float) {
      Assert.assertTrue(Math.abs((float) expected - (float) actual) < 0.000001f);
    } else if (expected instanceof Double) {
      Assert.assertTrue(Math.abs((double) expected - (double) actual) < 0.000001d);
    } else {
      Assert.assertEquals(expected, actual);
    }
  }

  protected void testFunctionSinglePartition(Supplier<AggregateFunction> supplier,
                                             Schema schema,
                                             Number expected,
                                             Iterator<?> iterator) {
    Number actual = (Number) getAggregateSinglePartition(supplier, schema, "x", iterator);
    System.out.println(expected + " - " + actual +  " - " + (expected.doubleValue() - actual.doubleValue()));
    if (expected instanceof Float) {
      Assert.assertTrue(Math.abs((float) expected - (float) actual) < 0.00001f);
    } else if (expected instanceof Double) {
      Assert.assertTrue(Math.abs((double) expected - (double) actual) < 0.00001d);
    } else {
      Assert.assertEquals(expected, actual);
    }
  }

  protected void testFunctionNPartitions(Supplier<AggregateFunction> supplier,
                                         Schema schema,
                                         Number expected,
                                         Iterator<?> iterator) {
    Number actual = (Number) getAggregateMultiplePartitions(supplier, schema, "x", iterator);
    System.out.println(expected + " - " + actual +  " - " + (expected.doubleValue() - actual.doubleValue()));
    if (expected instanceof Float) {
      Assert.assertTrue(Math.abs((float) expected - (float) actual) < 0.00005f);
    } else if (expected instanceof Double) {
      Assert.assertTrue(Math.abs((double) expected - (double) actual) < 0.00005d);
    } else {
      Assert.assertEquals(expected, actual);
    }
  }

  protected Iterator<Integer> getAgesIntegerIterator() {
    InputStream ages = getClass().getClassLoader().getResourceAsStream("aggregators/ages.txt");
    Objects.requireNonNull(ages);
    Scanner sc = new Scanner(ages);

    return new Iterator<Integer>() {
      @Override
      public boolean hasNext() {
        return sc.hasNextInt();
      }

      @Override
      public Integer next() {
        return sc.nextInt();
      }
    };
  }

  protected Iterator<Long> getAgesLongIterator() {
    InputStream ages = getClass().getClassLoader().getResourceAsStream("aggregators/ages.txt");
    Objects.requireNonNull(ages);
    Scanner sc = new Scanner(ages);

    return new Iterator<Long>() {
      @Override
      public boolean hasNext() {
        return sc.hasNextLong();
      }

      @Override
      public Long next() {
        return sc.nextLong();
      }
    };
  }

  protected Iterator<Float> getScoresFloatIterator() {
    InputStream scores = getClass().getClassLoader().getResourceAsStream("aggregators/scores.txt");
    Objects.requireNonNull(scores);
    Scanner sc = new Scanner(scores);

    return new Iterator<Float>() {
      @Override
      public boolean hasNext() {
        return sc.hasNextFloat();
      }

      @Override
      public Float next() {
        return sc.nextFloat();
      }
    };
  }

  protected Iterator<Double> getScoresDoubleIterator() {
    InputStream scores = getClass().getClassLoader().getResourceAsStream("aggregators/scores.txt");
    Objects.requireNonNull(scores);
    Scanner sc = new Scanner(scores);

    return new Iterator<Double>() {
      @Override
      public boolean hasNext() {
        return sc.hasNextDouble();
      }

      @Override
      public Double next() {
        return sc.nextDouble();
      }
    };
  }
}
