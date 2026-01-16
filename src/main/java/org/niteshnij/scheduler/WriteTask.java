package org.niteshnij.scheduler;

import org.niteshnij.core.KeyValueStore;

import java.util.concurrent.CompletableFuture;

/**
 * A write operation queued to the single-threaded write executor.
 * Failures complete the future exceptionally so callers can react.
 */
public class WriteTask implements Runnable {

    private final String key;
    private final String value;
    private final KeyValueStore keyValueStore;
    private final CompletableFuture<Void> future;

    public WriteTask(String key, String value, KeyValueStore keyValueStore, CompletableFuture<Void> future) {
        this.key = key;
        this.value = value;
        this.keyValueStore = keyValueStore;
        this.future = future;
    }

    @Override
    public void run() {
        try {
            keyValueStore.put(key, value);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    }

    public CompletableFuture<Void> getCompletableFuture() {
        return future;
    }
}
