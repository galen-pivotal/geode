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
  }

  @Test
  public void registersOneCounter() {
    StatisticDescriptor[] descriptors = counters(1);
    when(statistics.getType().getStatistics()).thenReturn(descriptors);
    when(descriptors[0].isCounter()).thenReturn(true);

    registrar.registerStatistics(statistics);

    String name = meterName(type, descriptors[0]);
    assertThat(registry.find(name).functionCounter()).isNotNull();
  }

  @Test
  public void registersManyCounters() {
    int statisticsCount = 5;
    StatisticDescriptor[] descriptors = counters(statisticsCount);
    when(statistics.getType().getStatistics()).thenReturn(descriptors);

    registrar.registerStatistics(statistics);

    for (int i = 0; i < statisticsCount; i++) {
      String name = meterName(type, descriptors[i]);
      assertThat(registry.find(name).functionCounter()).isNotNull();
    }
  }

  @Test
  public void registersOneGauge() {
    StatisticDescriptor[] descriptors = gauges(1);
    when(statistics.getType().getStatistics()).thenReturn(descriptors);

    registrar.registerStatistics(statistics);

    String name = meterName(type, descriptors[0]);
    assertThat(registry.find(name).gauge()).isNotNull();
  }

  @Test
  public void registersManyGauges() {
    int statisticsCount = 5;
    StatisticDescriptor[] descriptors = counters(statisticsCount);
    when(statistics.getType().getStatistics()).thenReturn(descriptors);

    registrar.registerStatistics(statistics);

    for (int i = 0; i < statisticsCount; i++) {
      String name = meterName(type, descriptors[i]);
      assertThat(registry.find(name).gauges()).isNotNull();
    }
  }

  @Test
  public void registersCounterAndGaugeMeters() {
    int counterCount = 5;
    int gaugeCount = 9;
    StatisticDescriptor[] descriptors = mixedMeters(counterCount, gaugeCount);
    when(statistics.getType().getStatistics()).thenReturn(descriptors);

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
    StatisticDescriptor[] descriptors = counters(statisticsCount);
    when(statistics.getType().getStatistics()).thenReturn(descriptors);

    for (int i = 0; i < statisticsCount; i++) {
      when(statistics.get(descriptors[i])).thenReturn(i);
    }

    registrar.registerStatistics(statistics);

    for (int i = 0; i < statisticsCount; i++) {
      String name = meterName(type, descriptors[i]);
      Meter meter = registry.find(name).meter();
      assertThat(meter).as(name).isNotNull();
      assertThat(valueOf(meter)).as("value of %s", name)
          .isEqualTo(i);
    }
  }

  @Test
  public void connectsGaugesToStatisticsValues() {
    int statisticsCount = 5;
    StatisticDescriptor[] descriptors = gauges(statisticsCount);
    when(statistics.getType().getStatistics()).thenReturn(descriptors);

    for (int i = 0; i < statisticsCount; i++) {
      when(statistics.get(descriptors[i])).thenReturn(i);
    }

    registrar.registerStatistics(statistics);

    for (int i = 0; i < statisticsCount; i++) {
      String name = meterName(type, descriptors[i]);
      Meter meter = registry.find(name).meter();
      assertThat(meter).as(name).isNotNull();
      assertThat(valueOf(meter)).as("value of %s", name)
          .isEqualTo(i);
    }
  }

  private static double valueOf(Meter meter) {
    Iterator<Measurement> iterator = meter.measure().iterator();
    assertThat(iterator.hasNext())
        .withFailMessage("%s has no measures", meter.getId())
        .isTrue();
    return iterator.next().getValue();
  }

  private static StatisticDescriptor[] mixedMeters(int counterCount, int gaugeCount) {
    List<StatisticDescriptor> counters = descriptors(counterCount, true);
    List<StatisticDescriptor> gauges = descriptors(gaugeCount, false);
    List<StatisticDescriptor> meters = new ArrayList<>();
    meters.addAll(counters);
    meters.addAll(gauges);
    shuffle(meters);
    return meters.toArray(new StatisticDescriptor[0]);
  }

  private static StatisticDescriptor[] counters(int count) {
    return descriptors(count, true)
        .toArray(new StatisticDescriptor[0]);
  }

  private static StatisticDescriptor[] gauges(int count) {
    return descriptors(count, false)
        .toArray(new StatisticDescriptor[0]);
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
}
