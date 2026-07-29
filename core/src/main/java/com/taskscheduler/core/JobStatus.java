package com.taskscheduler.core;

/**
 * Lifecycle states for a {@link Job}.
 *
 * <p>State machine:
 * <pre>
 *   PENDING     -&gt; SCHEDULED, CANCELLED
 *   SCHEDULED   -&gt; RUNNING, CANCELLED
 *   RUNNING     -&gt; SUCCEEDED, SCHEDULED (retry), DEAD_LETTERED
 *   SUCCEEDED, DEAD_LETTERED, CANCELLED are terminal (no outgoing transitions)
 * </pre>
 *
 * <p>There is deliberately no separate "FAILED" state: a transient failure
 * routes straight back to SCHEDULED so it can be retried. How many times a
 * job has failed lives on {@link Job#getRetryCount()}, not smeared across
 * the status enum. Only a failure that exhausts all retries produces a
 * terminal status (DEAD_LETTERED).
 *
 * <p>Note that RUNNING cannot transition to CANCELLED. That's intentional:
 * it means once a job is picked up by a worker, only that worker thread
 * ever changes its status again (until it's re-queued or terminal), which
 * is what makes the retry-vs-dead-letter decision race-free without an
 * extra lock.
 */
public enum JobStatus {
    PENDING,
    SCHEDULED,
    RUNNING,
    SUCCEEDED,
    DEAD_LETTERED,
    CANCELLED
}
