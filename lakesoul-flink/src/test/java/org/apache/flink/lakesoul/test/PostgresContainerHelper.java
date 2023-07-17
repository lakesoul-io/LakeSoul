package org.apache.flink.lakesoul.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class PostgresContainerHelper {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresContainerHelper.class);
    private static String CONTAINER_NAME = "lakesoul-test-pg";


    public static void setContainerName(String containerName) {
        CONTAINER_NAME = containerName;
    }

    public static String getStopCommand(String containerName) {
        return String.format("docker pause %s", containerName);
    }

    public static String getStartCommand(String containerName) {
        return String.format("docker unpause %s", containerName);
    }

    public static void runCommand(String command) throws IOException {

        Process process = Runtime.getRuntime().exec(command);
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        LOG.warn("======== stdout of Command: [" + command + "] ========");
        String line = "";
        while ((line = stdoutReader.readLine()) != null) {
            System.out.println(line);
        }
        LOG.warn("======== stdout End ========");

        BufferedReader stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        LOG.warn("======== stderr of Command: [" + command + "] ========");
        while ((line = stderrReader.readLine()) != null) {
            System.out.println(line);
        }
        LOG.warn("======== stderr End ========");
    }

    public static void stopPostgresForMills(long mills) {
        Thread thread = new Thread(() -> {
            try {
                runCommand(getStopCommand(CONTAINER_NAME));
            } catch (IOException e) {
                LOG.error("Failed to stop Postgres by " + e.getMessage());
            }
            try {
                Thread.sleep(mills);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                runCommand(getStartCommand(CONTAINER_NAME));
            } catch (IOException e) {
                LOG.error("Failed to restart Postgres by " + e.getMessage());
            }
        });
        thread.start();
    }

    public static void main(String[] args) throws IOException {
        runCommand("docker ps");
    }
}
