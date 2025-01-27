package org.apache.flume;

import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.sink.PollableSinkRunner;

abstract public class SinkRunner implements LifecycleAware {

  private Sink sink;

  public static SinkRunner forSink(Sink sink) {
    SinkRunner runner = null;

    if (sink instanceof PollableSink) {
      runner = new PollableSinkRunner();
      ((PollableSinkRunner) runner).setSink((PollableSink) sink);
    } else {
      throw new IllegalArgumentException("No known runner type for sink "
          + sink);
    }

    return runner;
  }

  public Sink getSink() {
    return sink;
  }

  public void setSink(Sink sink) {
    this.sink = sink;
  }

}
