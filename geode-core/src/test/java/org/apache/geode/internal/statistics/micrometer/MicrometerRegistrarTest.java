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
import java.util.concurrent.atomic.AtomicInteger;
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

    type = mock(StatisticsType.class);
    when(type.getName()).thenReturn("test.statistics.type");

    statistics = mock(Statistics.class);
    when(statistics.getType()).thenReturn(type);
    when(statistics.getTextId()).thenReturn("StatisticsTextId");

    counterDescriptors = descriptors(5, true);
    gaugeDescriptors = descriptors(9, false);

    AtomicInteger statValue = new AtomicInteger();
    for (StatisticDescriptor descriptor : counterDescriptors) {
      when(statistics.get(descriptor)).thenReturn(statValue.getAndIncrement());
    }
    for (StatisticDescriptor descriptor : gaugeDescriptors) {
      when(statistics.get(descriptor)).thenReturn(statValue.getAndIncrement());
    }
  }

  @Test
  public void registersCounters() {
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(counterDescriptors));

    registrar.registerStatistics(statistics);

    for (StatisticDescriptor descriptor : counterDescriptors) {
      String name = meterName(type, descriptor);
      assertThat(registry.find(name).functionCounter()).isNotNull();
    }
  }

  @Test
  public void registersGauges() {
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(gaugeDescriptors));

    registrar.registerStatistics(statistics);

    for (StatisticDescriptor descriptor : gaugeDescriptors) {
      String name = meterName(type, descriptor);
      assertThat(registry.find(name).gauges()).isNotNull();
    }
  }

  @Test
  public void assignsMeterTypeBasedOnWhetherStatisticDescriptorIsCounter() {
    List<StatisticDescriptor> descriptors = mixedDescriptors();
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));

    registrar.registerStatistics(statistics);

    for (StatisticDescriptor descriptor : descriptors) {
      Meter meter = registeredMeterFor(descriptor);
      if (descriptor.isCounter()) {
        assertThat(meter).as(meter.getId().toString())
            .isInstanceOf(FunctionCounter.class);
      } else {
        assertThat(meter).as(meter.getId().toString())
            .isInstanceOf(Gauge.class);
      }
    }
  }

  @Test
  public void connectsCountersToStatisticsValues() {
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(counterDescriptors));

    registrar.registerStatistics(statistics);

    for (StatisticDescriptor descriptor : counterDescriptors) {
      Meter meter = registeredMeterFor(descriptor);
      assertThat(valueOf(meter)).as("value of %s", meter.getId())
          .isEqualTo(statistics.get(descriptor).doubleValue());
    }
  }

  @Test
  public void connectsGaugesToStatisticsValues() {
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(gaugeDescriptors));

    registrar.registerStatistics(statistics);

    for (StatisticDescriptor descriptor : gaugeDescriptors) {
      Meter meter = registeredMeterFor(descriptor);
      assertThat(valueOf(meter)).as("value of %s", meter.getId())
          .isEqualTo(statistics.get(descriptor).doubleValue());
    }
  }

  @Test
  public void assignsStatisticsTextIDToMeterNameTag() {
    List<StatisticDescriptor> descriptors = mixedDescriptors();
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));

    registrar.registerStatistics(statistics);

    for (StatisticDescriptor descriptor : descriptors) {
      Meter meter = registeredMeterFor(descriptor);
      assertThat(meter.getId().getTag("name"))
          .as("name tag for %s", meter.getId())
          .isEqualTo(statistics.getTextId());
    }
  }

  @Test
  public void assignsStatisticsUnitToMeterBaseUnit() {
    List<StatisticDescriptor> descriptors = mixedDescriptors();
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));

    registrar.registerStatistics(statistics);

    for (StatisticDescriptor descriptor : descriptors) {
      Meter meter = registeredMeterFor(descriptor);
      assertThat(meter.getId().getBaseUnit())
          .as("units for %s", meter.getId())
          .isEqualTo(descriptor.getUnit());
    }
  }

  @Test
  public void deregistersMetersWhenStatisticsAreDestroyed() {
    List<StatisticDescriptor> descriptors = mixedDescriptors();
    when(statistics.getType().getStatistics()).thenReturn(arrayOf(descriptors));

    registrar.registerStatistics(statistics);
    registrar.deregisterStatistics(statistics);

    for (StatisticDescriptor descriptor : descriptors) {
      String name = meterName(type, descriptor);
      assertThat(registry.find(name).meter())
          .as("meter named %s", name)
          .isNull();
    }
  }

  private Meter registeredMeterFor(StatisticDescriptor descriptor) {
    String name = meterName(type, descriptor);
    Meter meter = registry.find(name).meter();
    assertThat(meter).as(name).isNotNull();
    return meter;
  }

  private static double valueOf(Meter meter) {
    Iterator<Measurement> iterator = meter.measure().iterator();
    assertThat(iterator.hasNext())
        .withFailMessage("%s has no measures", meter.getId())
        .isTrue();
    return iterator.next().getValue();
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
    shuffle(descriptors);
    return descriptors;
  }
}
