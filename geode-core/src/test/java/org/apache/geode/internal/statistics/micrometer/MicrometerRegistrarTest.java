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
import static org.apache.geode.internal.statistics.micrometer.MeterRegistrar.meterName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
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
  private Statistics statisticsInstance1;
  private MeterRegistrar registrar;
  private StatisticsType statisticsType;
  private MeterRegistry registry;
  private List<StatisticDescriptor> counterDescriptors;
  private List<StatisticDescriptor> gaugeDescriptors;
  private List<StatisticDescriptor> mixedDescriptors;

  @Before
  public void setup() {
    registry = new SimpleMeterRegistry();
    registrar = new MeterRegistrar(registry);

    counterDescriptors = descriptors(5, true);
    gaugeDescriptors = descriptors(9, false);
    mixedDescriptors = mixOf(counterDescriptors, gaugeDescriptors);

    statisticsType = mock(StatisticsType.class);
    when(statisticsType.getName()).thenReturn("statisticsType");

    statisticsInstance1 = statisticsInstance(statisticsType, "statisticsInstance1");
  }

  @Test
  public void registersCounters() {
    when(statisticsType.getStatistics()).thenReturn(arrayOf(counterDescriptors));

    registrar.registerStatistics(statisticsInstance1);

    for (StatisticDescriptor descriptor : counterDescriptors) {
      String name = meterName(statisticsType, descriptor);
      assertThat(registry.find(name).functionCounter()).isNotNull();
    }
  }

  @Test
  public void registersGauges() {
    when(statisticsType.getStatistics()).thenReturn(arrayOf(gaugeDescriptors));

    registrar.registerStatistics(statisticsInstance1);

    for (StatisticDescriptor descriptor : gaugeDescriptors) {
      String name = meterName(statisticsType, descriptor);
      assertThat(registry.find(name).gauges()).isNotNull();
    }
  }

  @Test
  public void assignsMeterTypeBasedOnWhetherStatisticDescriptorIsCounter() {
    when(statisticsType.getStatistics()).thenReturn(arrayOf(mixedDescriptors));

    registrar.registerStatistics(statisticsInstance1);

    for (StatisticDescriptor descriptor : mixedDescriptors) {
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
    when(statisticsType.getStatistics()).thenReturn(arrayOf(counterDescriptors));

    registrar.registerStatistics(statisticsInstance1);

    for (StatisticDescriptor descriptor : counterDescriptors) {
      Meter meter = registeredMeterFor(descriptor);
      assertThat(valueOf(meter)).as("value of %s", meter.getId())
          .isEqualTo(statisticsInstance1.get(descriptor).doubleValue());
    }
  }

  @Test
  public void connectsGaugesToStatisticsValues() {
    when(statisticsType.getStatistics()).thenReturn(arrayOf(gaugeDescriptors));

    registrar.registerStatistics(statisticsInstance1);

    for (StatisticDescriptor descriptor : gaugeDescriptors) {
      Meter meter = registeredMeterFor(descriptor);
      assertThat(valueOf(meter)).as("value of %s", meter.getId())
          .isEqualTo(statisticsInstance1.get(descriptor).doubleValue());
    }
  }

  @Test
  public void assignsStatisticsTextIDToMeterNameTag() {
    when(statisticsType.getStatistics()).thenReturn(arrayOf(mixedDescriptors));

    registrar.registerStatistics(statisticsInstance1);

    for (StatisticDescriptor descriptor : mixedDescriptors) {
      Meter meter = registeredMeterFor(descriptor);
      assertThat(meter.getId().getTag("name"))
          .as("name tag for %s", meter.getId())
          .isEqualTo(statisticsInstance1.getTextId());
    }
  }

  @Test
  public void registersMetersForMultipleStatisticsInstances() {
    when(statisticsType.getStatistics()).thenReturn(arrayOf(mixedDescriptors));

    Statistics statisticsInstance1 = statisticsInstance(statisticsType, "statisticsInstance1");
    Statistics statisticsInstance2 = statisticsInstance(statisticsType, "statisticsInstance2");

    registrar.registerStatistics(statisticsInstance1);
    registrar.registerStatistics(statisticsInstance2);

    for (StatisticDescriptor descriptor : mixedDescriptors) {
      String meterName = meterName(statisticsType, descriptor);
      Collection<Meter> meters = registry
          .find(meterName)
          .meters();
      assertThat(meters)
          .as("Meters named %s", meterName)
          .hasSize(2);
    }
  }

  @Test
  public void differentiatesMetersFromMultipleStatisticsByStatisticsTextId() {
    when(statisticsType.getStatistics()).thenReturn(arrayOf(mixedDescriptors));

    Statistics statisticsInstance1 = statisticsInstance(statisticsType, "statisticsInstance1");
    Statistics statisticsInstance2 = statisticsInstance(statisticsType, "statisticsInstance2");

    registrar.registerStatistics(statisticsInstance1);
    registrar.registerStatistics(statisticsInstance2);

    // One meter for each descriptor should be tagged with the textId of statisticsInstance1
    for (StatisticDescriptor descriptor : mixedDescriptors) {
      String meterName = meterName(statisticsType, descriptor);
      String tagForStatisticsInstance1 = statisticsInstance1.getTextId();
      Collection<Meter> meters = registry
          .find(meterName)
          .tags("name", tagForStatisticsInstance1)
          .meters();
      assertThat(meters)
          .as("Meters named %s tagged with textId %s", meterName, tagForStatisticsInstance1)
          .hasSize(1);
    }

    // One meter for each descriptor should be tagged with the textId of statisticsInstance2
    for (StatisticDescriptor descriptor : mixedDescriptors) {
      String meterName = meterName(statisticsType, descriptor);
      String tagForStatisticsInstance2 = statisticsInstance2.getTextId();
      Collection<Meter> meters = registry
          .find(meterName)
          .tags("name", tagForStatisticsInstance2)
          .meters();
      assertThat(meters)
          .as("Meters named %s tagged with textId %s", meterName, tagForStatisticsInstance2)
          .hasSize(1);
    }
  }

  @Test
  public void assignsStatisticsUnitToMeterBaseUnit() {
    when(statisticsType.getStatistics()).thenReturn(arrayOf(mixedDescriptors));

    registrar.registerStatistics(statisticsInstance1);

    for (StatisticDescriptor descriptor : mixedDescriptors) {
      Meter meter = registeredMeterFor(descriptor);
      assertThat(meter.getId().getBaseUnit())
          .as("units for %s", meter.getId())
          .isEqualTo(descriptor.getUnit());
    }
  }

  @Test
  public void deregistersMetersWhenStatisticsAreDestroyed() {
    when(statisticsType.getStatistics()).thenReturn(arrayOf(mixedDescriptors));

    registrar.registerStatistics(statisticsInstance1);
    registrar.deregisterStatistics(statisticsInstance1);

    for (StatisticDescriptor descriptor : mixedDescriptors) {
      String name = meterName(statisticsType, descriptor);
      assertThat(registry.find(name).meter())
          .as("meter named %s", name)
          .isNull();
    }
  }

  private Meter registeredMeterFor(StatisticDescriptor descriptor) {
    String name = meterName(statisticsType, descriptor);
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

  private static List<StatisticDescriptor> mixOf(List<StatisticDescriptor> descriptors1,
      List<StatisticDescriptor> descriptors2) {
    List<StatisticDescriptor> mixedDescriptors = new ArrayList<>();
    mixedDescriptors.addAll(descriptors1);
    mixedDescriptors.addAll(descriptors2);
    shuffle(mixedDescriptors);
    return mixedDescriptors;
  }

  private Statistics statisticsInstance(StatisticsType statisticsType, String textId) {
    Statistics statistics = mock(Statistics.class);
    when(statistics.getTextId()).thenReturn(textId);
    when(statistics.getType()).thenReturn(statisticsType);
    AtomicInteger statValue = new AtomicInteger();
    for (StatisticDescriptor descriptor : counterDescriptors) {
      when(statistics.get(descriptor)).thenReturn(statValue.getAndIncrement());
    }
    for (StatisticDescriptor descriptor : gaugeDescriptors) {
      when(statistics.get(descriptor)).thenReturn(statValue.getAndIncrement());
    }
    return statistics;
  }
}
