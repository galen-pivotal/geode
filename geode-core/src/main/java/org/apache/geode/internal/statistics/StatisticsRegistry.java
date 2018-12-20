package org.apache.geode.internal.statistics;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;

public class StatisticsRegistry implements StatisticsManager {
  private static final StatisticsTypeFactory tf = StatisticsTypeFactoryImpl.singleton();
  private final List<Statistics> statsList = new CopyOnWriteArrayList<>();
  private final String systemName;
  private final long startTime;
  private final AtomicLong statsListUniqueId = new AtomicLong(1);
  private int statsListModCount = 0;

  public StatisticsRegistry(String systemName, long startTime) {
    this.systemName = systemName;
    this.startTime = startTime;
  }

  @Override
  public String getName() {
    return systemName;
  }

  public Statistics findStatisticsByUniqueId(final long uniqueId) {
    for (Statistics s : statsList) {
      if (uniqueId == s.getUniqueId()) {
        return s;
      }
    }
    return null;
  }

  /**
   * For every registered statistic instance call the specified visitor. This method was added to
   * fix bug 40358
   */
  public void visitStatistics(StatisticsVisitor visitor) {
    for (Statistics s : statsList) {
      visitor.visit(s);
    }
  }

  @Override
  public int getStatListModCount() {
    return statsListModCount;
  }

  @Override
  public List<Statistics> getStatsList() {
    return statsList;
  }

  @Override
  public int getStatisticsCount() {
    return statsList.size();
  }

  @Override
  public Statistics findStatistics(long id) {
    for (Statistics s : statsList) {
      if (s.getUniqueId() == id) {
        return s;
      }
    }
    throw new RuntimeException(
        "Could not find statistics instance with id " + id);
  }

  @Override
  public boolean statisticsExists(long id) {
    for (Statistics s : statsList) {
      if (s.getUniqueId() == id) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Statistics[] getStatistics() {
    return statsList.toArray(new Statistics[0]);
  }

  @Override
  public Statistics createStatistics(StatisticsType type) {
    return createOsStatistics(type, null, 0, 0);
  }

  @Override
  public Statistics createStatistics(StatisticsType type, String textId) {
    return createOsStatistics(type, textId, 0, 0);
  }

  @Override
  public Statistics createStatistics(StatisticsType type, String textId, long numericId) {
    return createOsStatistics(type, textId, numericId, 0);
  }

  @Override
  public Statistics createOsStatistics(StatisticsType type, String textId, long numericId,
      int osStatFlags) {
    long myUniqueId = statsListUniqueId.getAndIncrement();
    Statistics result =
        new LocalStatisticsImpl(type, textId, numericId, myUniqueId, false, osStatFlags, this);
    addStatistics(result);
    return result;
  }

  @Override
  public Statistics[] findStatisticsByType(final StatisticsType type) {
    return statsList.stream()
        .filter(s -> type == s.getType())
        .toArray(Statistics[]::new);
  }

  @Override
  public Statistics[] findStatisticsByTextId(final String textId) {
    return statsList.stream()
        .filter(s -> textId.equals(s.getTextId()))
        .toArray(Statistics[]::new);
  }

  @Override
  public Statistics[] findStatisticsByNumericId(final long numericId) {
    return statsList.stream()
        .filter(s -> numericId == s.getNumericId())
        .toArray(Statistics[]::new);
  }

  /**
   * for internal use only. Its called by {@link LocalStatisticsImpl#close}.
   */
  @Override
  public void destroyStatistics(Statistics stats) {
    synchronized (statsList) {
      if (statsList.remove(stats)) {
        statsListModCount++;
      }
    }
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type) {
    return createAtomicStatistics(type, null, 0);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId) {
    return createAtomicStatistics(type, textId, 0);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId, long numericId) {
    long myUniqueId = statsListUniqueId.getAndIncrement();
    Statistics result = StatisticsImpl.createAtomicNoOS(type, textId, numericId, myUniqueId, this);
    addStatistics(result);
    return result;
  }

  /**
   * Creates or finds a StatisticType for the given shared class.
   */
  @Override
  public StatisticsType createType(String name, String description, StatisticDescriptor[] stats) {
    return tf.createType(name, description, stats);
  }

  @Override
  public StatisticsType findType(String name) {
    return tf.findType(name);
  }

  @Override
  public StatisticsType[] createTypesFromXml(Reader reader) throws IOException {
    return tf.createTypesFromXml(reader);
  }

  @Override
  public StatisticDescriptor createIntCounter(String name, String description, String units) {
    return tf.createIntCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createLongCounter(String name, String description, String units) {
    return tf.createLongCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name, String description, String units) {
    return tf.createDoubleCounter(name, description, units);
  }

  @Override
  public StatisticDescriptor createIntGauge(String name, String description, String units) {
    return tf.createIntGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createLongGauge(String name, String description, String units) {
    return tf.createLongGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name, String description, String units) {
    return tf.createDoubleGauge(name, description, units);
  }

  @Override
  public StatisticDescriptor createIntCounter(String name, String description, String units,
      boolean largerBetter) {
    return tf.createIntCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createLongCounter(String name, String description, String units,
      boolean largerBetter) {
    return tf.createLongCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name, String description, String units,
      boolean largerBetter) {
    return tf.createDoubleCounter(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createIntGauge(String name, String description, String units,
      boolean largerBetter) {
    return tf.createIntGauge(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createLongGauge(String name, String description, String units,
      boolean largerBetter) {
    return tf.createLongGauge(name, description, units, largerBetter);
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name, String description, String units,
      boolean largerBetter) {
    return tf.createDoubleGauge(name, description, units, largerBetter);
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  private void addStatistics(Statistics statistics) {
    synchronized (statsList) {
      statsList.add(statistics);
      statsListModCount++;
    }
  }
}
