/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.statistics.micrometer;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.ToDoubleFunction;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;

/**
 * Maintains a meter for each statistic value in each registered {@link Statistics} instance.
 * <p>
 * Each meter is given a name of the form <em>typeName.descriptorName</em>, where
 * <em>typeName</em> is the name of the statistics instance's {@link StatisticsType}, and
 * <em>descriptorName</em> is the name of the {@link StatisticDescriptor} that defines the
 * statistic value.
 * <p>
 * Each meter is given a tag with key {@code "name"} and value <em>statisticsId</em>, where
 * <em>statisticsId</em> is the statistics instance's {@code textID}.
 * <p>
 * Each meter's type is determined by the associated statistic descriptor's {@link
 * StatisticDescriptor#isCounter() isCounter()} method. If {@code isCounter()} is true, the meter is
 * a {@link FunctionCounter}. Otherwise the meter is a {@link Gauge}.
 * <p>
 * When sampled, each meter obtains its value extracting the statistics instance's value for the
 * associated descriptor.
 */
public class MeterRegistrar {
  private final MeterRegistry registry;

  /**
   * Creates a registrar that maintains a meter in the given registry for each statistic value in
   * each registered {@link Statistics} instance.
   *
   * @param registry the registry in which to maintain meters
   */
  public MeterRegistrar(MeterRegistry registry) {
    this.registry = registry;
  }

  /**
   * Creates a meter for each statistic value defined by the given {@link Statistics} instance and
   * adds each meter to the meter registry.
   *
   * @param statistics the statistics for which to create and register meters
   */
  public void registerStatistics(Statistics statistics) {
    StatisticsType type = statistics.getType();
    for (StatisticDescriptor descriptor : type.getStatistics()) {
      registerMeter(descriptor, statistics, type);
    }
  }

  /**
   * Removes the meters associated with the given statistics instance from the meter registry.
   *
   * @param statistics the statistics for which to deregister meters
   */
  public void deregisterStatistics(Statistics statistics) {
    StatisticsType type = statistics.getType();
    Arrays.stream(type.getStatistics())
        .map(descriptor -> findMeter(descriptor, statistics))
        .filter(Objects::nonNull)
        .forEach(registry::remove);
  }

  private void registerMeter(StatisticDescriptor descriptor, Statistics statistics,
      StatisticsType type) {
    ToDoubleFunction<Statistics> extractor = s -> s.get(descriptor).doubleValue();
    String meterName = meterName(type, descriptor);
    String meterUnit = descriptor.getUnit();
    if (descriptor.isCounter()) {
      registerCounter(meterName, statistics, extractor, meterUnit);
    } else {
      registerGauge(meterName, statistics, extractor, meterUnit);
    }
  }

  private void registerGauge(String name, Statistics source,
      ToDoubleFunction<Statistics> extractor,
      String unit) {
    String nameTag = source.getTextId();
    Gauge.builder(name, source, extractor)
        .baseUnit(unit)
        .tag("name", nameTag)
        .register(registry);
  }

  private void registerCounter(String name, Statistics source,
      ToDoubleFunction<Statistics> extractor,
      String unit) {
    String nameTag = source.getTextId();
    FunctionCounter.builder(name, source, extractor)
        .baseUnit(unit)
        .tag("name", nameTag)
        .register(registry);
  }

  static String meterName(StatisticsType type, StatisticDescriptor descriptor) {
    String typeName = type.getName();
    String statName = descriptor.getName();
    return String.format("%s.%s", typeName, statName);
  }

  private Meter findMeter(StatisticDescriptor descriptor,
      Statistics statistics) {
    return registry.find(meterName(statistics.getType(), descriptor))
        .tags("name", statistics.getTextId())
        .meter();
  }
}
