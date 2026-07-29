package com.taskscheduler.core;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holding area for jobs that exhausted their retries. Deliberately dumb
 * storage -- replay logic (resetting a job and re-submitting it) lives in
 * {@link TaskScheduler}, so this class doesn't need to know about
 * {@link JobQueue} at all.
 */
public final class DeadLetterQueue {

    private final Map<UUID, Job> jobs = new ConcurrentHashMap<>();

    void add(Job job) {
        jobs.put(job.getId(), job);
    }

    Optional<Job> remove(UUID jobId) {
        return Optional.ofNullable(jobs.remove(jobId));
    }

    public Optional<Job> get(UUID jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    /** Most recently dead-lettered first. */
    public List<Job> list() {
        return jobs.values().stream()
                .sorted(Comparator.comparingLong(Job::getDeadLetteredAt).reversed())
                .toList();
    }

    public int size() {
        return jobs.size();
    }
}
