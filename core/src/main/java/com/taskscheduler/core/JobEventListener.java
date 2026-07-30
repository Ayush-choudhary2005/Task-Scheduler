package com.taskscheduler.core;

/**
 * Observer interface. Implementations are invoked synchronously, on
 * whichever thread caused the status change (the submitting thread for
 * submit/cancel, a worker thread for started/succeeded/retry/dead-letter
 * transitions).
 *
 * <p>Because of that, a listener that does blocking I/O (e.g. a JPA write)
 * adds latency directly to the worker thread's loop. That's fine at low
 * volume, but a listener meant for production use under heavy concurrent
 * job completions should hand the event off to its own single-threaded
 * executor and return immediately, rather than write to a database inline
 * on the caller's thread. The core module intentionally doesn't make that
 * call for you -- see the persistence listener added in the Spring layer.
 */
@FunctionalInterface
public interface JobEventListener  {
    void onJobEvent(JobEvent event);
}
