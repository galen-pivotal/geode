package org.apache.geode.internal.statistics;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;

public class DummyStatisticsRegistry extends StatisticsRegistry {
  public DummyStatisticsRegistry(long systemId, String systemName, long startTime) {
    super(systemId, systemName, startTime);
  }

  @Override
  public Statistics createOsStatistics(StatisticsType type, String textId, long numericId,
                                       int osStatFlags) {
    return new DummyStatisticsImpl(type, textId, numericId);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId, long numericId) {
    return new DummyStatisticsImpl(type, textId, numericId);
  }
}
