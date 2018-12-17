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
      ToDoubleFunction<Statistics> extractor = s -> s.get(descriptor).doubleValue();
      if (descriptor.isCounter()) {
        FunctionCounter.builder(meterName(type, descriptor), statistics, extractor)
            .register(registry);
      } else {
        Gauge.builder(meterName(type, descriptor), statistics, extractor).register(registry);
      }
    }
  }

  static String meterName(StatisticsType type, StatisticDescriptor sd) {
    String typeName = type.getName();
    String statName = sd.getName();
    return String.format("%s.%s", typeName, statName);
  }
}
