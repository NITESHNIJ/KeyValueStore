package org.niteshnij.scheduler;

import org.niteshnij.core.KeyValueStore;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * A read operation submitted to the read thread pool.
 * Retries up to 3 times on transient IO failures before propagating.
 */
public class ReadTask implements Callable<String> {

    private static final int MAX_RETRIES = 3;

    private final String key;
    private final KeyValueStore keyValueStore;
    private final CompletableFuture<String> future;
    private int retryCount = 0;

    public ReadTask(String key, KeyValueStore keyValueStore, CompletableFuture<String> future) {
        this.key = key;
        this.keyValueStore = keyValueStore;
        this.future = future;
    }

    public CompletableFuture<String> getCompletableFuture() {
        return future;
    }

    @Override
    public String call() throws Exception {
        try {
            return keyValueStore.get(key);
        } catch (IOException e) {
            if (retryCount < MAX_RETRIES) {
                retryCount++;
                System.out.println("Retrying read for key: " + key + " (attempt " + retryCount + ")");
                return call();
            }
            System.err.println("Read failed for key: " + key + " — " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
