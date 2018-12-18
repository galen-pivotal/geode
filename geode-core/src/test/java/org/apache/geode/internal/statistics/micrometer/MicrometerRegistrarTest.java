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
  private List<StatisticDescriptor> counterDescriptors;
  private List<StatisticDescriptor> gaugeDescriptors;

  @Before
  public void setup() {
    registry = new SimpleMeterRegistry();
    registrar = new MicrometerRegistrar(registry);

    statistics = mock(Statistics.class);
    type = mock(StatisticsType.class);
    when(type.getName()).thenReturn("test.statistics.type");

    when(statistics.getType()).thenReturn(type);
    when(statistics.getTextId()).thenReturn("StatisticsTextId");

    counterDescriptors = counters(5);
    gaugeDescriptors = gauges(9);
  }

  @Test
  public void registersCounters() {
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(counterDescriptors));

    registrar.registerStatistics(statistics);

    for (StatisticDescriptor counterDescriptor : counterDescriptors) {
      String name = meterName(type, counterDescriptor);
      assertThat(registry.find(name).functionCounter()).isNotNull();
    }
  }

  @Test
  public void registersGauges() {
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(gaugeDescriptors));

    registrar.registerStatistics(statistics);

    for (StatisticDescriptor gaugeDescriptor : gaugeDescriptors) {
      String name = meterName(type, gaugeDescriptor);
      assertThat(registry.find(name).gauges()).isNotNull();
    }
  }

  @Test
  public void assignsMeterTypeBasedOnWhetherStatisticDescriptorIsCounter() {
    List<StatisticDescriptor> descriptors = mixedDescriptors();
    shuffle(descriptors);

    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));

    registrar.registerStatistics(statistics);

    for (StatisticDescriptor descriptor : descriptors) {
      String name = meterName(type, descriptor);
      Meter meter = registry.find(name).meter();
      if (descriptor.isCounter()) {
        assertThat(meter).as(name).isInstanceOf(FunctionCounter.class);
      } else {
        assertThat(meter).as(name).isInstanceOf(Gauge.class);
      }
    }
  }

  @Test
  public void connectsCountersToStatisticsValues() {
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(counterDescriptors));

    for (int i = 0; i < counterDescriptors.size(); i++) {
      when(statistics.get(counterDescriptors.get(i))).thenReturn(i);
    }

    registrar.registerStatistics(statistics);

    for (int i = 0; i < counterDescriptors.size(); i++) {
      String name = meterName(type, counterDescriptors.get(i));
      Meter meter = registry.find(name).meter();
      assertThat(meter).as(name).isNotNull();
      assertThat(valueOf(meter)).as("value of %s", name)
          .isEqualTo(i);
    }
  }

  @Test
  public void connectsGaugesToStatisticsValues() {
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(gaugeDescriptors));

    for (int i = 0; i < gaugeDescriptors.size(); i++) {
      when(statistics.get(gaugeDescriptors.get(i))).thenReturn(i);
    }

    registrar.registerStatistics(statistics);

    for (int i = 0; i < gaugeDescriptors.size(); i++) {
      String name = meterName(type, gaugeDescriptors.get(i));
      Meter meter = registry.find(name).meter();
      assertThat(meter).as(name).isNotNull();
      assertThat(valueOf(meter)).as("value of %s", meter.getId())
          .isEqualTo(i);
    }
  }

  @Test
  public void assignsStatisticsTextIDToMeterNameTag() {
    List<StatisticDescriptor> descriptors = mixedDescriptors();
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));
    when(statistics.getTextId()).thenReturn("StatisticsTextId");

    registrar.registerStatistics(statistics);

    for (StatisticDescriptor descriptor : descriptors) {
      String name = meterName(type, descriptor);
      Meter meter = registry.find(name).meter();
      assertThat(meter).as(name).isNotNull();
      assertThat(meter.getId().getTag("name"))
          .as("name tag for meter %s", meter.getId())
          .isEqualTo(statistics.getTextId());
    }
  }

  @Test
  public void assignsStatisticsUnitToMeterBaseUnit() {
    List<StatisticDescriptor> descriptors = mixedDescriptors();
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));

    for (int i = 0; i < descriptors.size(); i++) {
      when(descriptors.get(i).getUnit()).thenReturn("units-" + i);
    }

    registrar.registerStatistics(statistics);

    for (int i = 0; i < descriptors.size(); i++) {
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
    when(descriptor.getUnit()).thenReturn(name + "unit-" + id);
    return descriptor;
  }

  private static StatisticDescriptor[] arrayOf(List<StatisticDescriptor> descriptors) {
    return descriptors.toArray(new StatisticDescriptor[0]);
  }

  private List<StatisticDescriptor> mixedDescriptors() {
    List<StatisticDescriptor> descriptors = new ArrayList<>();
    descriptors.addAll(gaugeDescriptors);
    descriptors.addAll(counterDescriptors);
    return descriptors;
  }
}
