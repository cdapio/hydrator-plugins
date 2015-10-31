package co.cask.hydrator.sinks;

import co.cask.cdap.api.metrics.Metrics;

/**
 * No op metrics implementation for tests.
 */
public class NoopMetrics implements Metrics {
  public static final Metrics INSTANCE = new NoopMetrics();

  @Override
  public void count(String s, int i) {
    // no-op
  }

  @Override
  public void gauge(String s, long l) {
    // no-op
  }
}