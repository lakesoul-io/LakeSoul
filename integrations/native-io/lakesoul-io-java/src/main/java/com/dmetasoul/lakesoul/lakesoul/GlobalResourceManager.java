package com.dmetasoul.lakesoul.lakesoul;

import com.dmetasoul.lakesoul.lakesoul.io.jnr.JnrLoader;
import com.dmetasoul.lakesoul.lakesoul.io.jnr.LibLakeSoulIO;

import jnr.ffi.Pointer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class GlobalResourceManager {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalResourceManager.class);
    private static final List<AutoCloseable> RESOURCES = new CopyOnWriteArrayList<>();
    private static final AtomicBoolean SHUTDOWN_STARTED = new AtomicBoolean(false);

    static {
        Runtime.getRuntime()
                .addShutdownHook(new Thread(GlobalResourceManager::shutdown, "GlobalShutdownHook"));

        LibLakeSoulIO nativeLibrary = JnrLoader.get();
        Pointer buildInfo = nativeLibrary.lakesoul_io_build_info();
        if (buildInfo == null) {
            LOG.warn("LakeSoul native build information is unavailable");
        } else {
            LOG.info("LakeSoul native build information: {}", buildInfo.getString(0));
        }
    }

    public static void register(AutoCloseable resource) {
        RESOURCES.add(resource);
    }

    public static void shutdown() {
        if (!SHUTDOWN_STARTED.compareAndSet(false, true)) {
            return;
        }

        LOG.info("Global shutdown sequence started");

        for (int i = RESOURCES.size() - 1; i >= 0; i--) {
            AutoCloseable resource = RESOURCES.get(i);
            String resourceName = resource.getClass().getSimpleName();
            try {
                LOG.debug("Closing resource: {}", resourceName);
                resource.close();
            } catch (Exception e) {
                LOG.error("Error closing resource: {}", resourceName, e);
            }
        }
        LOG.info("Global shutdown sequence finished");
    }
}
