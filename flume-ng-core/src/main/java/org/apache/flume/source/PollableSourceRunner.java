package org.apache.flume.source;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.CounterGroup;
import org.apache.flume.PollableSource;
import org.apache.flume.SourceRunner;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * An implementation of {@link SourceRunner} that can drive a
 * {@link PollableSource}.
 * </p>
 * <p>
 * A {@link PollableSourceRunner} wraps a {@link PollableSource} in the required
 * run loop in order for it to operate. Internally, metrics and counters are
 * kept such that a source that returns a {@link PollableSource.Status} of
 * {@code BACKOFF} causes the run loop to do exactly that. There's a maximum
 * backoff period of 500ms. A source that returns {@code READY} is immediately
 * invoked. Note that {@code BACKOFF} is merely a hint to the runner; it need
 * not be strictly adhered to.
 * </p>
 */
public class PollableSourceRunner extends SourceRunner {

  private static final Logger logger = LoggerFactory
      .getLogger(PollableSourceRunner.class);
  private static final long backoffSleepIncrement = 100;
  private static final long maxBackoffSleep = 500;

  private AtomicBoolean shouldStop;

  private CounterGroup counterGroup;
  private PollingRunner runner;
  private Thread runnerThread;
  private LifecycleState lifecycleState;

  public PollableSourceRunner() {
    shouldStop = new AtomicBoolean();
    counterGroup = new CounterGroup();
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void start() {
    PollableSource source = (PollableSource) getSource();

    source.start();

    runner = new PollingRunner();

    runner.source = source;
    runner.counterGroup = counterGroup;
    runner.shouldStop = shouldStop;

    runnerThread = new Thread(runner);
    runnerThread.start();

    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop() {

    runner.shouldStop.set(true);

    try {
      runnerThread.interrupt();
      runnerThread.join();
    } catch (InterruptedException e) {
      logger
          .warn(
              "Interrupted while waiting for polling runner to stop. Please report this.",
              e);
      Thread.currentThread().interrupt();
    }

    getSource().stop();

    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public String toString() {
    return "PollableSourceRunner: { source:" + getSource() + " counterGroup:"
        + counterGroup + " }";
  }

  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  public static class PollingRunner implements Runnable {

    private PollableSource source;
    private AtomicBoolean shouldStop;
    private CounterGroup counterGroup;

    @Override
    public void run() {
      logger.debug("Polling runner starting. Source:{}", source);

      while (!shouldStop.get()) {
        counterGroup.incrementAndGet("runner.polls");

        try {
          if (source.process().equals(PollableSource.Status.BACKOFF)) {
            counterGroup.incrementAndGet("runner.backoffs");

            Thread.sleep(Math.min(
                counterGroup.incrementAndGet("runner.backoffs.consecutive")
                    * backoffSleepIncrement, maxBackoffSleep));
          } else {
            counterGroup.set("runner.backoffs.consecutive", 0L);
          }
        } catch (InterruptedException e) {
          logger.info("Source runner interrupted. Exiting");
        } catch (Exception e) {
          logger.error("Unable to process event. Exception follows.", e);
          counterGroup.incrementAndGet("runner.failures");
        }
      }

      logger.debug("Polling runner exiting. Metrics:{}", counterGroup);
    }

  }

}
