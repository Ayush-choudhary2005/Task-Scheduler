package com.taskscheduler.core;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Computes retry backoff delays: exponential growth capped at a maximum,
 * with "full jitter" -- the actual delay is sampled uniformly from
 * {@code [0, cappedDelay]} rather than being the capped value itself.
 *
 * <p>Full jitter (as opposed to no jitter, or +/- a fixed percentage) is
 * what actually breaks up a thundering herd: if 50 jobs fail at the same
 * instant against the same downstream dependency, a fixed backoff curve
 * has all 50 retry in lockstep again. Sampling the delay spreads them out.
 *
 * <p>{@link ThreadLocalRandom} is used deliberately instead of a shared
 * {@link java.util.Random}: multiple worker threads compute backoff
 * concurrently, and a shared Random serializes on its internal CAS under
 * contention.
 */
public final class RetryPolicy {

    private final long baseDelayMillis;
    private final long maxDelayMillis;
    private final double multiplier;

    private RetryPolicy(long baseDelayMillis, long maxDelayMillis, double multiplier) {
        if (baseDelayMillis <= 0) {
            throw new IllegalArgumentException("baseDelayMillis must be positive");
        }
        if (maxDelayMillis < baseDelayMillis) {
            throw new IllegalArgumentException("maxDelayMillis must be >= baseDelayMillis");
        }
        if (multiplier <= 1.0) {
            throw new IllegalArgumentException("multiplier must be > 1.0");
        }
        this.baseDelayMillis = baseDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
        this.multiplier = multiplier;
    }

    public static RetryPolicy defaultPolicy() {
        return new RetryPolicy(500L, 30_000L, 2.0);
    }

    public static RetryPolicy of(long baseDelayMillis, long maxDelayMillis, double multiplier) {
        return new RetryPolicy(baseDelayMillis, maxDelayMillis, multiplier);
    }

    /**
     * @param attempt the 1-based retry attempt number (1 = first retry after
     *                the original failure)
     * @return delay in milliseconds before the retry should run
     */
    public long nextDelayMillis(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
        double raw = baseDelayMillis * Math.pow(multiplier, attempt - 1);
        long capped = raw >= (double) maxDelayMillis ? maxDelayMillis : (long) raw;
        capped = Math.max(capped, 1L); // never a zero-width jitter range
        return ThreadLocalRandom.current().nextLong(capped + 1);
    }
}
