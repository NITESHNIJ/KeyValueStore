package org.niteshnij;

import org.niteshnij.scheduler.MasterTask;

import java.io.File;
import java.util.Arrays;
import java.util.Scanner;

/**
 * Interactive CLI for the key-value store.
 *
 * Commands:
 *   put    — store a key/value pair
 *   get    — retrieve a value by key
 *   merge  — manually trigger compaction (normally happens automatically)
 *   exit   — shut down cleanly
 */
public class Main {

    public static void main(String[] args) {
        try {
            Scanner scanner = new Scanner(System.in);
            MasterTask masterTask = new MasterTask();

            System.out.println("KeyValueStore ready. Commands: put | get | merge | exit");

            while (true) {
                System.out.print("> ");
                String command = scanner.nextLine().trim();

                switch (command) {
                    case "exit":
                        System.out.println("Shutting down.");
                        return;

                    case "put": {
                        System.out.print("key: ");
                        String key = scanner.nextLine().trim();
                        System.out.print("value: ");
                        String value = scanner.nextLine().trim();
                        masterTask.submitWriteTask(key, value)
                                .thenRun(() -> System.out.println("Stored."));
                        break;
                    }

                    case "get": {
                        System.out.print("key: ");
                        String key = scanner.nextLine().trim();
                        masterTask.submitReadTask(key)
                                .thenAccept(value -> {
                                    if (value == null) {
                                        System.out.println("(not found)");
                                    } else {
                                        System.out.println(key + " = " + value);
                                    }
                                });
                        break;
                    }

                    case "merge": {
                        File dataDir = new File("data");
                        if (!dataDir.exists() || !dataDir.isDirectory()) {
                            System.out.println("No data directory found.");
                            break;
                        }
                        String[] files = Arrays.stream(dataDir.list())
                                .filter(f -> f.endsWith(".db"))
                                .toArray(String[]::new);
                        Arrays.sort(files);

                        if (files.length < 2) {
                            System.out.println("Need at least 2 segment files to merge.");
                            break;
                        }
                        System.out.println("Triggering merge across " + files.length + " files...");
                        masterTask.submitMergeTask();
                        System.out.println("Merge complete.");
                        break;
                    }

                    default:
                        System.out.println("Unknown command. Try: put | get | merge | exit");
                }
            }
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
