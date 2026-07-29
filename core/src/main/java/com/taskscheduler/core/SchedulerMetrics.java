package com.taskscheduler.core;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Counters backing the live dashboard / metrics endpoint.
 *
 * <p>{@link LongAdder} is used for the high-write-frequency counters since
 * it's built for exactly this access pattern (many threads incrementing,
 * occasional reads) and scales better than {@code AtomicLong} under
 * contention, because concurrent writes don't all CAS the same memory cell.
 */
public final class SchedulerMetrics {

    private final LongAdder submitted = new LongAdder();
    private final LongAdder succeeded = new LongAdder();
    private final LongAdder retried = new LongAdder();
    private final LongAdder deadLettered = new LongAdder();
    private final LongAdder cancelled = new LongAdder();
    private final AtomicInteger activeWorkers = new AtomicInteger(0);

    void incrementSubmitted() {
        submitted.increment();
    }

    void incrementSucceeded() {
        succeeded.increment();
    }

    void incrementRetried() {
        retried.increment();
    }

    void incrementDeadLettered() {
        deadLettered.increment();
    }

    void incrementCancelled() {
        cancelled.increment();
    }

    void workerStarted() {
        activeWorkers.incrementAndGet();
    }

    void workerFinished() {
        activeWorkers.decrementAndGet();
    }

    public Snapshot snapshot(int queueDepth, int deadLetterSize) {
        return new Snapshot(
                submitted.sum(),
                succeeded.sum(),
                retried.sum(),
                deadLettered.sum(),
                cancelled.sum(),
                activeWorkers.get(),
                queueDepth,
                deadLetterSize
        );
    }

    public record Snapshot(
            long submitted,
            long succeeded,
            long retried,
            long deadLettered,
            long cancelled,
            int activeWorkers,
            int queueDepth,
            int deadLetterSize
    ) {
    }
}
