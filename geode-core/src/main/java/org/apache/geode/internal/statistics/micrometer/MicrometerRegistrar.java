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

import java.util.function.ToDoubleFunction;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;

public class MicrometerRegistrar {
  private final MeterRegistry registry;

  public MicrometerRegistrar(MeterRegistry registry) {
    this.registry = registry;
  }

  public void registerStatistics(Statistics statistics) {
    StatisticsType type = statistics.getType();
    for (StatisticDescriptor descriptor : type.getStatistics()) {
      registerMeter(descriptor, statistics, type);
    }
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
    Gauge.builder(name, source, extractor)
        .baseUnit(unit)
        .register(registry);
  }

  private void registerCounter(String name, Statistics source,
      ToDoubleFunction<Statistics> extractor,
      String unit) {
    FunctionCounter.builder(name, source, extractor)
        .baseUnit(unit)
        .register(registry);
  }

  static String meterName(StatisticsType type, StatisticDescriptor descriptor) {
    String typeName = type.getName();
    String statName = descriptor.getName();
    return String.format("%s.%s", typeName, statName);
  }
}
