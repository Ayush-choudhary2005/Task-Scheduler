package com.taskscheduler.core.demo;

import com.taskscheduler.core.Job;
import com.taskscheduler.core.JobHandlerRegistry;
import com.taskscheduler.core.RetryPolicy;
import com.taskscheduler.core.TaskScheduler;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hands-on demo -- NOT part of the engine itself, and not something the real
 * system will use once the API layer exists in step 2. This exists purely
 * so you can watch the scheduler actually do something on a console before
 * adding Spring, a database, or a REST API on top of it.
 *
 * <p>Notice this class lives in a different package (com.taskscheduler.core.demo)
 * from the engine classes (com.taskscheduler.core). It only ever touches
 * public methods -- Job's internal mutators like transitionTo() are
 * package-private, so this file physically cannot call them, the same way
 * the future Spring layer won't be able to either. That's the encapsulation
 * boundary working as intended.
 */
public final class Main {

    public static void main(String[] args) throws InterruptedException {
        JobHandlerRegistry registry = new JobHandlerRegistry();

        // "email" always succeeds immediately.
        registry.register("email", job ->
                System.out.println("  [handler:email] sent email for job " + job.getName()));

        // "flaky" fails the first two attempts, then succeeds on the third --
        // this is what proves retry + exponential backoff actually work.
        Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        registry.register("flaky", job -> {
            int attempt = attempts
                    .computeIfAbsent(job.getId().toString(), k -> new AtomicInteger(0))
                    .incrementAndGet();
            System.out.println("  [handler:flaky] attempt " + attempt + " for job " + job.getName());
            if (attempt < 3) {
                throw new RuntimeException("simulated transient failure");
            }
        });

        // "always-fails" never succeeds -- proves dead-lettering after maxRetries.
        registry.register("always-fails", job -> {
            throw new RuntimeException("simulated permanent failure");
        });

        // Fast backoff (200ms base, 2s cap) just so the demo doesn't take forever.
        RetryPolicy retryPolicy = RetryPolicy.of(200, 2_000, 2.0);
        TaskScheduler scheduler = new TaskScheduler(3, registry, retryPolicy);

        // Observer in action: every status change gets printed as it happens.
        scheduler.addListener(event ->
                System.out.printf("EVENT  %-10s %s -> %-13s  %s%n",
                        event.jobId().toString().substring(0, 8),
                        event.oldStatus(), event.newStatus(), event.message()));

        scheduler.start();

        // --- Priority ordering ---
        // Both submitted with zero delay, low priority first. If priority
        // ordering is correct, "urgent-report" should start running before
        // "low-priority-email" even though it was submitted second.
        scheduler.submit(Job.builder().name("low-priority-email").type("email").priority(1).build());
        scheduler.submit(Job.builder().name("urgent-report").type("email").priority(10).build());

        // --- Delayed job --- scheduled 1.5s in the future.
        scheduler.submit(Job.builder().name("scheduled-digest").type("email")
                .priority(5).delay(1500, TimeUnit.MILLISECONDS).build());

        // --- Retry demo --- fails twice, succeeds on the third attempt.
        scheduler.submit(Job.builder().name("flaky-webhook").type("flaky")
                .priority(5).maxRetries(5).build());

        // --- Dead-letter demo --- never succeeds; maxRetries=2 so it dies fast.
        scheduler.submit(Job.builder().name("doomed-job").type("always-fails")
                .priority(5).maxRetries(2).build());

        // --- Cancellation demo --- submit with a long delay, then cancel it
        // before it ever gets a chance to run.
        UUID cancelMeId = scheduler.submit(Job.builder().name("cancel-me").type("email")
                .priority(5).delay(5, TimeUnit.SECONDS).build());
        boolean cancelled = scheduler.cancel(cancelMeId);
        System.out.println("Cancelled cancel-me: " + cancelled);

        // Let everything finish running.
        Thread.sleep(4000);

        System.out.println();
        System.out.println("---- Metrics ----");
        System.out.println(scheduler.metricsSnapshot());

        System.out.println("---- Dead letters ----");
        scheduler.listDeadLetters().forEach(job ->
                System.out.println(job.getName() + " : " + job.getLastError()));

        scheduler.shutdown();
    }
}