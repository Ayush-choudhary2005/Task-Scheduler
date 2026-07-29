package com.taskscheduler.core;

import java.time.Instant;
import java.util.UUID;

/**
 * Fired on every status change. This is the entire surface the persistence
 * layer (or anything else) needs to observe -- the core scheduler has no
 * idea whether zero, one, or a hundred listeners are attached, and no idea
 * a database exists at all.
 */
public record JobEvent(
        UUID jobId,
        JobStatus oldStatus,
        JobStatus newStatus,
        Instant timestamp,
        String message
) {
    static JobEvent of(Job job, JobStatus oldStatus, String message) {
        return new JobEvent(job.getId(), oldStatus, job.getStatus(), Instant.now(), message);
    }
}
