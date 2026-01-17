package org.niteshnij.scheduler;

import org.niteshnij.core.KeyValueStore;
import org.niteshnij.core.KeyValueStoreImpl;

import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Coordinates concurrent access to the key-value store.
 *
 * - Reads run on a fixed pool of NUM_READ_THREADS threads.
 * - Writes are serialized through a single-threaded executor to guarantee
 *   append ordering without extra locking in the hot path.
 * - Compaction is triggered probabilistically (~1% of writes) so it happens
 *   in the background without a dedicated scheduling thread.
 */
public class MasterTask {

    private static final int NUM_READ_THREADS = 5;
    private static final double MERGE_TRIGGER_PROBABILITY = 0.01;

    private final Queue<ReadTask> readQueue = new ConcurrentLinkedQueue<>();
    private final Queue<WriteTask> writeQueue = new LinkedList<>(); // guarded by synchronized block

    private final ExecutorService readExecutor = Executors.newFixedThreadPool(NUM_READ_THREADS);
    private final ExecutorService writeExecutor = Executors.newSingleThreadExecutor();

    private final KeyValueStore keyValueStore;

    public MasterTask() throws FileNotFoundException {
        this.keyValueStore = new KeyValueStoreImpl();
    }

    public CompletableFuture<String> submitReadTask(String key) {
        CompletableFuture<String> future = new CompletableFuture<>();
        readQueue.add(new ReadTask(key, keyValueStore, future));
        readExecutor.submit(this::processReadTask);
        return future;
    }

    public CompletableFuture<Void> submitWriteTask(String key, String value) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        writeQueue.add(new WriteTask(key, value, keyValueStore, future));
        writeExecutor.submit(this::processWriteTask);

        // Stochastic compaction trigger — avoids dedicated scheduler overhead.
        if (Math.random() < MERGE_TRIGGER_PROBABILITY) {
            submitMergeTask();
        }
        return future;
    }

    private void processWriteTask() {
        synchronized (writeQueue) {
            WriteTask task = writeQueue.poll();
            if (task != null) {
                task.run();
                task.getCompletableFuture().complete(null);
            }
        }
    }

    private void processReadTask() {
        ReadTask task = readQueue.poll();
        if (task == null) return;
        try {
            String value = task.call();
            task.getCompletableFuture().complete(value);
        } catch (Exception e) {
            task.getCompletableFuture().completeExceptionally(e);
        }
    }

    public void submitMergeTask() {
        keyValueStore.compactAndMerge();
    }
}
