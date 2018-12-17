package org.apache.geode.internal.statistics.micrometer;

import static org.apache.geode.internal.statistics.micrometer.MicrometerRegistrar.meterName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.IntStream;

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
  public void registersEachCounter() {
    int statisticsCount = 5;
    StatisticDescriptor[] descriptors = counters(statisticsCount);
    when(statistics.getType().getStatistics()).thenReturn(descriptors);
    when(descriptors[0].isCounter()).thenReturn(true);

    registrar.registerStatistics(statistics);

    for (int i = 0; i < statisticsCount; i++) {
      String name = meterName(type, descriptors[i]);
      assertThat(registry.find(name).functionCounter()).isNotNull();
    }
  }

  @Test
  public void registersOneGauge() {
    StatisticDescriptor[] descriptors = gauges(1);
    when(descriptors[0].isCounter()).thenReturn(false);
    when(statistics.getType().getStatistics()).thenReturn(descriptors);

    registrar.registerStatistics(statistics);

    String name = meterName(type, descriptors[0]);
    assertThat(registry.find(name).gauge()).isNotNull();
  }

  private static StatisticDescriptor[] counters(int count) {
    return descriptors(count, true);
  }

  private static StatisticDescriptor[] gauges(int count) {
    return descriptors(count, false);
  }

  private static StatisticDescriptor[] descriptors(int count, boolean isCounter) {
    return IntStream.range(0, count)
        .mapToObj(id -> descriptor(id, isCounter))
        .toArray(StatisticDescriptor[]::new);
  }

  private static StatisticDescriptor descriptor(int id, boolean isCounter) {
    StatisticDescriptor descriptor = mock(StatisticDescriptor.class);
    when(descriptor.getName()).thenReturn("stat" + id);
    when(descriptor.isCounter()).thenReturn(isCounter);
    return descriptor;
  }
}
