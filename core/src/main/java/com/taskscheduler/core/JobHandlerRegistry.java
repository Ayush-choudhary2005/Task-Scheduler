package com.taskscheduler.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Strategy registry mapping a job's {@code type} string to the
 * {@link JobHandler} that knows how to execute it.
 */
public final class JobHandlerRegistry {

    private final Map<String, JobHandler> handlers = new ConcurrentHashMap<>();

    public JobHandlerRegistry register(String type, JobHandler handler) {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("type must not be blank");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler must not be null");
        }
        handlers.put(type, handler);
        return this;
    }

    public void unregister(String type) {
        handlers.remove(type);
    }

    public boolean isRegistered(String type) {
        return handlers.containsKey(type);
    }

    /**
     * @throws IllegalStateException if no handler is registered for the job's type
     */
    public JobHandler resolve(String type) {
        JobHandler handler = handlers.get(type);
        if (handler == null) {
            throw new IllegalStateException("No JobHandler registered for type '" + type + "'");
        }
        return handler;
    }
}
