package io.jatoms;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import one.microstream.storage.embedded.types.EmbeddedStorage;
import one.microstream.storage.embedded.types.EmbeddedStorageManager;

/**
 * Minimal example for excessive RAM usage.
 * When started this app takes more and more of the available RAM, although I would expect the RAM to be freed by Microstream?
 */
public class App {
    public static void main(String[] args) {
        final EmbeddedStorageManager storageManager = EmbeddedStorage.start();

        Data data = (Data)storageManager.root();
        if(data == null) {
            // Create a root instance
            data = new Data();
            storageManager.setRoot(data);
            storageManager.storeRoot();
        }

        Data finalData = data;
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        
        exec.scheduleAtFixedRate(() -> {
            // replace the internal map of data with a new one every 2 seconds
            finalData.update(createSampleData(100_000));
            // and store it
            storageManager.store(finalData);
        }, 0, 2, TimeUnit.SECONDS);
        
        // wait for the user to abort this process
        try(Scanner scanner = new Scanner(System.in)) {
            System.out.println("Hit Enter to Exit");
            scanner.nextLine(); 
            System.out.println("Exiting"); 
            storageManager.shutdown();
            System.exit(0);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    private static Map<String, String> createSampleData(int size) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String key = UUID.randomUUID().toString();
            String value = key + "_test_" + i;
            result.put(key, value);
        }
        return result;
    }

    public static class Data {
        private Map<String, String> data = new HashMap<>();
    
        public void update(Map<String, String> newData) {
            this.data = newData;
        }
    }
}
