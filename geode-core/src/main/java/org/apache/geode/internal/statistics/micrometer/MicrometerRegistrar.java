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
