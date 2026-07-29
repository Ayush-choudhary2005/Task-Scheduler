package com.taskscheduler.core;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wires the queue, worker pool, handler registry, retry policy, dead letter
 * queue, metrics, and event listeners together. This is the only class
 * external code (the Spring layer, tests, etc.) needs to talk to.
 */
public final class TaskScheduler {

    private final JobQueue jobQueue = new JobQueue();
    private final DeadLetterQueue deadLetterQueue = new DeadLetterQueue();
    private final SchedulerMetrics metrics = new SchedulerMetrics();
    private final Map<UUID, Job> jobRegistry = new ConcurrentHashMap<>();
    private final List<JobEventListener> listeners = new CopyOnWriteArrayList<>();

    private final JobHandlerRegistry handlerRegistry;
    private final RetryPolicy retryPolicy;
    private final int workerCount;
    private final ExecutorService workerPool;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public TaskScheduler(int workerCount, JobHandlerRegistry handlerRegistry, RetryPolicy retryPolicy) {
        if (workerCount < 1) {
            throw new IllegalArgumentException("workerCount must be >= 1");
        }
        this.workerCount = workerCount;
        this.handlerRegistry = handlerRegistry;
        this.retryPolicy = retryPolicy;
        this.workerPool = Executors.newFixedThreadPool(workerCount, r -> {
            Thread t = new Thread(r);
            t.setName("scheduler-worker-" + t.threadId());
            t.setDaemon(true);
            return t;
        });
    }

    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return; // already started
        }
        for (int i = 0; i < workerCount; i++) {
            workerPool.submit(this::workerLoop);
        }
    }

    public synchronized void shutdown() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        jobQueue.shutdown();
        workerPool.shutdownNow();
        try {
            workerPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- Public API ----

    public UUID submit(Job job) {
        jobRegistry.put(job.getId(), job);
        job.transitionTo(JobStatus.SCHEDULED);
        metrics.incrementSubmitted();
        publish(job, JobStatus.PENDING, "submitted");
        jobQueue.put(job);
        return job.getId();
    }

    public boolean cancel(UUID jobId) {
        Job job = jobRegistry.get(jobId);
        if (job == null) {
            return false;
        }
        JobStatus before = job.getStatus();
        boolean cancelled = job.transitionTo(JobStatus.CANCELLED);
        if (cancelled) {
            jobQueue.remove(job); // best-effort; see JobQueue.remove javadoc
            metrics.incrementCancelled();
            publish(job, before, "cancelled");
        }
        return cancelled;
    }

    public Optional<Job> getJob(UUID jobId) {
        return Optional.ofNullable(jobRegistry.get(jobId));
    }

    public List<Job> listJobs() {
        return List.copyOf(jobRegistry.values());
    }

    public List<Job> listDeadLetters() {
        return deadLetterQueue.list();
    }

    /**
     * Resets a dead-lettered job and resubmits it. A dead-lettered job is
     * terminal in the state machine (see {@link JobStatus}), so replay
     * builds a fresh {@link Job} carrying over the original's type,
     * payload, priority, and retry budget, rather than trying to force a
     * terminal job back to life -- that keeps the state machine and the
     * retry counter both starting clean.
     */
    public boolean replay(UUID jobId) {
        Optional<Job> maybeJob = deadLetterQueue.remove(jobId);
        if (maybeJob.isEmpty()) {
            return false;
        }
        Job original = maybeJob.get();
        Job replayed = Job.builder()
                .name(original.getName())
                .type(original.getType())
                .payload(original.getPayload())
                .priority(original.getPriority())
                .maxRetries(original.getMaxRetries())
                .build();
        submit(replayed);
        return true;
    }

    public SchedulerMetrics.Snapshot metricsSnapshot() {
        return metrics.snapshot(jobQueue.size(), deadLetterQueue.size());
    }

    public void addListener(JobEventListener listener) {
        listeners.add(listener);
    }

    public void removeListener(JobEventListener listener) {
        listeners.remove(listener);
    }

    // ---- Worker loop ----

    private void workerLoop() {
        while (running.get()) {
            Job job;
            try {
                job = jobQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            JobStatus before = job.getStatus();
            if (!job.transitionTo(JobStatus.RUNNING)) {
                // Lost the race to a concurrent cancellation -- drop it silently.
                continue;
            }
            publish(job, before, "started");
            metrics.workerStarted();

            try {
                JobHandler handler = handlerRegistry.resolve(job.getType());
                handler.handle(job);
                job.transitionTo(JobStatus.SUCCEEDED);
                metrics.incrementSucceeded();
                publish(job, JobStatus.RUNNING, "succeeded");
            } catch (Exception e) {
                handleFailure(job, e);
            } finally {
                metrics.workerFinished();
            }
        }
    }

    /**
     * Decides retry vs. dead-letter. No CAS/locking is needed here beyond
     * what {@code transitionTo} already does: a job is RUNNING only while
     * owned by exactly one worker thread (RUNNING can't be cancelled, see
     * {@link JobStatus}), so this method never races with another thread
     * over the same job.
     */
    private void handleFailure(Job job, Exception e) {
        job.setLastError(e);
        int attempt = job.incrementRetryCount();

        if (attempt > job.getMaxRetries()) {
            job.transitionTo(JobStatus.DEAD_LETTERED);
            job.markDeadLettered();
            deadLetterQueue.add(job);
            metrics.incrementDeadLettered();
            publish(job, JobStatus.RUNNING, "dead-lettered after " + attempt + " attempts: " + e.getMessage());
        } else {
            long delay = retryPolicy.nextDelayMillis(attempt);
            job.setNextExecutionTime(System.currentTimeMillis() + delay);
            job.transitionTo(JobStatus.SCHEDULED);
            metrics.incrementRetried();
            publish(job, JobStatus.RUNNING,
                    "retry " + attempt + "/" + job.getMaxRetries() + " scheduled in " + delay + "ms: " + e.getMessage());
            jobQueue.put(job);
        }
    }

    private void publish(Job job, JobStatus oldStatus, String message) {
        JobEvent event = JobEvent.of(job, oldStatus, message);
        for (JobEventListener listener : listeners) {
            listener.onJobEvent(event);
        }
    }
}
