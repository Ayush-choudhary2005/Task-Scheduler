package com.taskscheduler.core;

import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A unit of work submitted to the scheduler.
 *
 * <p>Job is mutable by design (status, retry count, and next-execution-time
 * all change over its lifetime) but every mutable field is either an atomic
 * type or guarded by the CAS-based state machine in {@link #transitionTo},
 * so a Job can be safely handed between the thread that submits/cancels it,
 * the promoter thread that moves it between queues, and the worker thread
 * that executes it, without external locking.
 *
 * <p>Mutator methods are package-private: only scheduling code in this
 * package may change a Job's internal state. External callers (the future
 * API layer) only ever see the read-only getters.
 */
public final class Job implements Delayed {

    private final UUID id;
    private final String name;
    private final String type;
    private final Object payload;
    private final int priority;
    private final int maxRetries;
    private final long createdAt;

    private final AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.PENDING);
    private final AtomicInteger retryCount = new AtomicInteger(0);
    private volatile long nextExecutionTime;
    private volatile String lastError;
    private volatile long deadLetteredAt = -1L;

    private Job(Builder builder) {
        this.id = UUID.randomUUID();
        this.name = builder.name;
        this.type = builder.type;
        this.payload = builder.payload;
        this.priority = builder.priority;
        this.maxRetries = builder.maxRetries;
        this.createdAt = System.currentTimeMillis();
        this.nextExecutionTime = this.createdAt + builder.initialDelayMillis;
    }

    public static Builder builder() {
        return new Builder();
    }

    // ---- Delayed (used only while the job sits in JobQueue's DelayQueue stage) ----

    @Override
    public long getDelay(TimeUnit unit) {
        long remainingMillis = nextExecutionTime - System.currentTimeMillis();
        return unit.convert(remainingMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        if (other == this) {
            return 0;
        }
        long diff = getDelay(TimeUnit.MILLISECONDS) - other.getDelay(TimeUnit.MILLISECONDS);
        return Long.signum(diff);
    }

    // ---- State machine ----

    /**
     * Attempts to move this job from its current status to {@code target}.
     * Returns false, with no side effects, if the transition isn't legal
     * from whatever the status happens to be at the moment of the call.
     * Callers must check the return value rather than assume success --
     * another thread may have concurrently changed the status (e.g. an API
     * thread cancelling a job just before a worker tries to start it).
     */
    boolean transitionTo(JobStatus target) {
        while (true) {
            JobStatus current = status.get();
            if (!isValidTransition(current, target)) {
                return false;
            }
            if (status.compareAndSet(current, target)) {
                return true;
            }
            // Lost a race with another thread's transition; re-read and retry.
        }
    }

    private static boolean isValidTransition(JobStatus from, JobStatus to) {
        return switch (from) {
            case PENDING -> to == JobStatus.SCHEDULED || to == JobStatus.CANCELLED;
            case SCHEDULED -> to == JobStatus.RUNNING || to == JobStatus.CANCELLED;
            case RUNNING -> to == JobStatus.SUCCEEDED
                    || to == JobStatus.SCHEDULED
                    || to == JobStatus.DEAD_LETTERED;
            case SUCCEEDED, DEAD_LETTERED, CANCELLED -> false;
        };
    }

    // ---- Mutators (package-private: core scheduling code only) ----

    int incrementRetryCount() {
        return retryCount.incrementAndGet();
    }

    void setNextExecutionTime(long epochMillis) {
        this.nextExecutionTime = epochMillis;
    }

    void setLastError(Throwable t) {
        this.lastError = (t == null) ? null : (t.getClass().getSimpleName() + ": " + t.getMessage());
    }

    void markDeadLettered() {
        this.deadLetteredAt = System.currentTimeMillis();
    }

    // ---- Getters ----

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Object getPayload() {
        return payload;
    }

    public int getPriority() {
        return priority;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getNextExecutionTime() {
        return nextExecutionTime;
    }

    public JobStatus getStatus() {
        return status.get();
    }

    public int getRetryCount() {
        return retryCount.get();
    }

    public String getLastError() {
        return lastError;
    }

    public long getDeadLetteredAt() {
        return deadLetteredAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Job job)) {
            return false;
        }
        return id.equals(job.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "Job{id=%s, name=%s, type=%s, priority=%d, status=%s, retry=%d/%d}"
                .formatted(id, name, type, priority, status.get(), retryCount.get(), maxRetries);
    }

    // ---- Builder ----

    public static final class Builder {
        private String name;
        private String type;
        private Object payload;
        private int priority = 0;
        private int maxRetries = 3;
        private long initialDelayMillis = 0L;

        private Builder() {
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder payload(Object payload) {
            this.payload = payload;
            return this;
        }

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries cannot be negative");
            }
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder delay(long amount, TimeUnit unit) {
            if (amount < 0) {
                throw new IllegalArgumentException("delay cannot be negative");
            }
            this.initialDelayMillis = unit.toMillis(amount);
            return this;
        }

        public Job build() {
            if (type == null || type.isBlank()) {
                throw new IllegalStateException("Job.type is required (it selects the JobHandler)");
            }
            if (name == null || name.isBlank()) {
                name = type;
            }
            return new Job(this);
        }
    }
}
