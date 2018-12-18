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

import static java.util.Collections.shuffle;
import static java.util.stream.Collectors.toList;
import static org.apache.geode.internal.statistics.micrometer.MicrometerRegistrar.meterName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;

public class MicrometerRegistrarTest {
  private Statistics statistics;
  private MicrometerRegistrar registrar;
  private StatisticsType type;
  private MeterRegistry registry;

  @Before
  public void setup() {
    registry = new SimpleMeterRegistry();
    registrar = new MicrometerRegistrar(registry);

    statistics = mock(Statistics.class);
    type = mock(StatisticsType.class);
    when(type.getName()).thenReturn("test.statistics.type");
    when(statistics.getType()).thenReturn(type);
    when(statistics.getTextId()).thenReturn("StatisticsTextId");
  }

  @Test
  public void registersOneCounter() {
    List<StatisticDescriptor> descriptors = counters(1);
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));
    when(descriptors.get(0).isCounter()).thenReturn(true);

    registrar.registerStatistics(statistics);

    String name = meterName(type, descriptors.get(0));
    assertThat(registry.find(name).functionCounter()).isNotNull();
  }

  @Test
  public void registersManyCounters() {
    int statisticsCount = 5;
    List<StatisticDescriptor> descriptors = counters(statisticsCount);
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));

    registrar.registerStatistics(statistics);

    for (int i = 0; i < statisticsCount; i++) {
      String name = meterName(type, descriptors.get(i));
      assertThat(registry.find(name).functionCounter()).isNotNull();
    }
  }

  @Test
  public void registersOneGauge() {
    List<StatisticDescriptor> descriptors = gauges(1);
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));

    registrar.registerStatistics(statistics);

    String name = meterName(type, descriptors.get(0));
    assertThat(registry.find(name).gauge()).isNotNull();
  }

  @Test
  public void registersManyGauges() {
    int statisticsCount = 5;
    List<StatisticDescriptor> descriptors = counters(statisticsCount);
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));

    registrar.registerStatistics(statistics);

    for (int i = 0; i < statisticsCount; i++) {
      String name = meterName(type, descriptors.get(i));
      assertThat(registry.find(name).gauges()).isNotNull();
    }
  }

  @Test
  public void assignsMeterTypeBasedOnWhetherStatisticDescriptorIsCounter() {
    int counterCount = 5;
    int gaugeCount = 9;
    List<StatisticDescriptor> descriptors = mixedMeters(counterCount, gaugeCount);
    shuffle(descriptors);
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));

    registrar.registerStatistics(statistics);

    for (StatisticDescriptor descriptor : descriptors) {
      String name = meterName(type, descriptor);
      Meter meter = registry.find(name).meter();
      if (descriptor.isCounter()) {
        assertThat(meter).as(name)
            .isInstanceOf(FunctionCounter.class);
      } else {
        assertThat(meter).as(name)
            .isInstanceOf(Gauge.class);
      }
    }
  }

  @Test
  public void connectsCountersToStatisticsValues() {
    int statisticsCount = 5;
    List<StatisticDescriptor> descriptors = counters(statisticsCount);
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));

    for (int i = 0; i < statisticsCount; i++) {
      when(statistics.get(descriptors.get(i))).thenReturn(i);
    }

    registrar.registerStatistics(statistics);

    for (int i = 0; i < statisticsCount; i++) {
      String name = meterName(type, descriptors.get(i));
      Meter meter = registry.find(name).meter();
      assertThat(meter).as(name).isNotNull();
      assertThat(valueOf(meter)).as("value of %s", name)
          .isEqualTo(i);
    }
  }

  @Test
  public void connectsGaugesToStatisticsValues() {
    int statisticsCount = 5;
    List<StatisticDescriptor> descriptors = gauges(statisticsCount);
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));

    for (int i = 0; i < statisticsCount; i++) {
      when(statistics.get(descriptors.get(i))).thenReturn(i);
    }

    registrar.registerStatistics(statistics);

    for (int i = 0; i < statisticsCount; i++) {
      String name = meterName(type, descriptors.get(i));
      Meter meter = registry.find(name).meter();
      assertThat(meter).as(name).isNotNull();
      assertThat(valueOf(meter)).as("value of %s", meter.getId())
          .isEqualTo(i);
    }
  }

  @Test
  public void assignsStatisticsTextIDToMeterNameTag() {
    int statisticsCount = 5;
    List<StatisticDescriptor> descriptors = mixedMeters(4, 9);
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));
    when(statistics.getTextId()).thenReturn("StatisticsTextId");

    registrar.registerStatistics(statistics);

    for (int i = 0; i < statisticsCount; i++) {
      String name = meterName(type, descriptors.get(i));
      Meter meter = registry.find(name).meter();
      assertThat(meter).as(name).isNotNull();
      assertThat(meter.getId().getTag("name"))
          .as("name tag for meter %s", meter.getId())
          .isEqualTo(statistics.getTextId());
    }
  }

  @Test
  public void assignsStatisticsUnitToMeterBaseUnit() {
    int statisticsCount = 5;
    List<StatisticDescriptor> descriptors = mixedMeters(4, 9);
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));

    for (int i = 0; i < statisticsCount; i++) {
      when(descriptors.get(i).getUnit()).thenReturn("units-" + i);
    }

    registrar.registerStatistics(statistics);

    for (int i = 0; i < statisticsCount; i++) {
      String name = meterName(type, descriptors.get(i));
      Meter meter = registry.find(name).meter();
      assertThat(meter).as(name).isNotNull();
      assertThat(meter.getId().getBaseUnit())
          .as("units for meter %s", meter.getId())
          .isEqualTo("units-" + i);
    }
  }

  private static double valueOf(Meter meter) {
    Iterator<Measurement> iterator = meter.measure().iterator();
    assertThat(iterator.hasNext())
        .withFailMessage("%s has no measures", meter.getId())
        .isTrue();
    return iterator.next().getValue();
  }

  private static List<StatisticDescriptor> mixedMeters(int counterCount, int gaugeCount) {
    List<StatisticDescriptor> counters = descriptors(counterCount, true);
    List<StatisticDescriptor> gauges = descriptors(gaugeCount, false);
    List<StatisticDescriptor> meters = new ArrayList<>();
    meters.addAll(counters);
    meters.addAll(gauges);
    return meters;
  }

  private static List<StatisticDescriptor> counters(int count) {
    return descriptors(count, true);
  }

  private static List<StatisticDescriptor> gauges(int count) {
    return descriptors(count, false);
  }

  private static List<StatisticDescriptor> descriptors(int count, boolean isCounter) {
    return IntStream.range(0, count)
        .mapToObj(id -> descriptor(id, isCounter))
        .collect(toList());
  }

  private static StatisticDescriptor descriptor(int id, boolean isCounter) {
    String name = isCounter ? "counter-" : "gauge-";
    StatisticDescriptor descriptor = mock(StatisticDescriptor.class);
    when(descriptor.getName()).thenReturn(name + id);
    when(descriptor.isCounter()).thenReturn(isCounter);
    return descriptor;
  }

  private static StatisticDescriptor[] arrayOf(List<StatisticDescriptor> descriptors) {
    return descriptors.toArray(new StatisticDescriptor[0]);
  }
}
