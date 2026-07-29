package com.taskscheduler.core;

/**
 * Strategy interface: the actual work a {@link Job} performs.
 * Implementations are registered against a job "type" string in
 * {@link JobHandlerRegistry}, so the scheduler itself never knows what a
 * job actually does.
 *
 * <p>Throwing from {@link #handle} marks the attempt as failed and hands
 * control to the scheduler's retry/dead-letter logic. Handlers should not
 * catch and swallow exceptions they want retried.
 */
@FunctionalInterface
public interface JobHandler {
    void handle(Job job) throws Exception;
}
