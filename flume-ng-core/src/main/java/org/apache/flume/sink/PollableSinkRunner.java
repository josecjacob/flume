package org.apache.flume.sink;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.CounterGroup;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSink;
import org.apache.flume.SinkRunner;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PollableSinkRunner extends SinkRunner {

  private static final Logger logger = LoggerFactory
      .getLogger(PollableSinkRunner.class);

  private CounterGroup counterGroup;
  private PollingRunner runner;
  private Thread runnerThread;
  private LifecycleState lifecycleState;

  public PollableSinkRunner() {
    counterGroup = new CounterGroup();
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void start() {
    PollableSink sink = (PollableSink) getSink();

    sink.start();

    runner = new PollingRunner();

    runner.sink = sink;
    runner.counterGroup = counterGroup;
    runner.shouldStop = new AtomicBoolean();

    runnerThread = new Thread(runner);
    runnerThread.start();

    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop() {

    getSink().stop();

    if (runnerThread != null) {
      runner.shouldStop.set(true);
      runnerThread.interrupt();

      while (runnerThread.isAlive()) {
        try {
          logger.debug("Waiting for runner thread to exit");
          runnerThread.join(500);
        } catch (InterruptedException e) {
          logger
              .debug(
                  "Interrupted while waiting for runner thread to exit. Exception follows.",
                  e);
        }
      }
    }

    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public String toString() {
    return "PollableSinkRunner: { sink:" + getSink() + " counterGroup:"
        + counterGroup + " }";
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  public static class PollingRunner implements Runnable {

    private PollableSink sink;
    private AtomicBoolean shouldStop;
    private CounterGroup counterGroup;

    @Override
    public void run() {
      logger.debug("Polling sink runner starting");

      while (!shouldStop.get()) {
        try {
          if (sink.process().equals(PollableSink.Status.BACKOFF)) {
            counterGroup.incrementAndGet("runner.backoffs");
            /* Should this be configurable? */
            Thread.sleep(500);
          }
        } catch (InterruptedException e) {
          logger.debug("Interrupted while processing an event. Exiting.");
          counterGroup.incrementAndGet("runner.interruptions");
        } catch (EventDeliveryException e) {
          logger.error("Unable to deliver event. Exception follows.", e);
          counterGroup.incrementAndGet("runner.errors");
        }
      }

      logger.debug("Polling runner exiting. Metrics:{}", counterGroup);
    }

  }

}
