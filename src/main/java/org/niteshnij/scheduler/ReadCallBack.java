package org.niteshnij.scheduler;

/**
 * Optional callback for read results. Kept for extensibility but
 * the primary path uses CompletableFuture via ReadTask.
 */
public interface ReadCallBack {
    void onReadComplete(String result);
}
