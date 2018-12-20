package org.apache.geode.internal.statistics;

import org.apache.geode.Statistics;
import org.apache.geode.internal.statistics.StatisticsRegistry;

/**
 * Used to "visit" each instance of Statistics registered with
 *
 * @see StatisticsRegistry#visitStatistics
 */
public interface StatisticsVisitor {

  void visit(Statistics stat);
}
