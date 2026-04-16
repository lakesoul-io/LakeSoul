package com.dmetasoul.lakesoul.lakesoul;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class GlobalResourceManager {
    private static final List<AutoCloseable> RESOURCES = new CopyOnWriteArrayList<>();
    private static final AtomicBoolean SHUTDOWN_STARTED = new AtomicBoolean(false);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(GlobalResourceManager::shutdown, "GlobalShutdownHook"));
    }

    public static void register(AutoCloseable resource) {
        RESOURCES.add(resource);
    }

    public static void shutdown() {
        if (!SHUTDOWN_STARTED.compareAndSet(false, true)) return;

        System.err.println("--- Global Shutdown Sequence Started ---");

        for (int i = RESOURCES.size() - 1; i >= 0; i--) {
            AutoCloseable res = RESOURCES.get(i);
            try {
                System.err.println("Closing resource: " + res.getClass().getSimpleName());
                res.close();
            } catch (Exception e) {
                System.err.println("Error closing " + res.getClass().getSimpleName());
                e.printStackTrace();
            }
        }
        System.err.println("--- Global Shutdown Sequence Finished ---");
    }
}