package com.taskscheduler.core;

import java.util.Comparator;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Two-stage queue that makes priority ordering correct even for jobs with
 * zero (or already-elapsed) delay.
 *
 * <p>A single {@link PriorityBlockingQueue} can't respect "run this in
 * 500ms" -- it has no concept of delay. A single {@link DelayQueue} can't
 * respect priority -- elements become eligible purely by elapsed time, and
 * jobs that are all "ready right now" come out in whatever order the
 * DelayQueue's internal heap happens to store them, not by priority.
 *
 * <p>So every job first waits in a DelayQueue until its scheduled time
 * arrives (immediately, for zero-delay jobs). A single background
 * "promoter" thread blocks on {@code delayQueue.take()} and, the instant a
 * job becomes eligible, moves it into a PriorityBlockingQueue ordered by
 * priority. Workers only ever call {@link #take()}, which blocks on the
 * ready queue.
 *
 * <p><b>Why this handoff is race-free:</b> workers never poll the delay
 * queue, and never check "is anything ready" by inspecting queue state
 * themselves -- they only ever block on {@code readyQueue.take()}.
 * {@code BlockingQueue}'s {@code put()}/{@code take()} contract guarantees
 * that once the promoter successfully {@code put()}s an element, any
 * thread already blocked in {@code take()} (or one that calls it
 * afterwards) will see it. There is no window where a ready job is
 * invisible to every worker.
 */
public final class JobQueue {

    private final DelayQueue<Job> delayQueue = new DelayQueue<>();
    private final PriorityBlockingQueue<Job> readyQueue;
    private final Thread promoterThread;
    private volatile boolean running = true;

    public JobQueue() {
        Comparator<Job> byPriorityThenAge =
                Comparator.comparingInt(Job::getPriority).reversed()
                        .thenComparingLong(Job::getCreatedAt);
        this.readyQueue = new PriorityBlockingQueue<>(64, byPriorityThenAge);

        this.promoterThread = new Thread(this::promoteLoop, "job-queue-promoter");
        this.promoterThread.setDaemon(true);
        this.promoterThread.start();
    }

    private void promoteLoop() {
        while (running) {
            try {
                Job job = delayQueue.take(); // blocks until a job's delay elapses
                readyQueue.put(job);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** Enqueues a job (or re-enqueues it for retry). Always enters via the delay stage. */
    public void put(Job job) {
        delayQueue.put(job);
    }

    /** Blocks until a job is ready to run, then returns it, highest priority first. */
    public Job take() throws InterruptedException {
        return readyQueue.take();
    }

    /**
     * Best-effort removal, used by cancellation. It's fine for this to not
     * find the job (e.g. it's already RUNNING, or was promoted between the
     * caller reading its status and this call) -- the authoritative
     * cancellation guard is the CAS in {@link Job#transitionTo}; this is
     * just cleanup so a cancelled job doesn't sit around waiting to be
     * picked up and then silently discarded by the worker.
     */
    public boolean remove(Job job) {
        return delayQueue.remove(job) || readyQueue.remove(job);
    }

    public int size() {
        return delayQueue.size() + readyQueue.size();
    }

    public void shutdown() {
        running = false;
        promoterThread.interrupt();
    }
}
