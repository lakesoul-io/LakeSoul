// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class TickSource implements SourceFunction<String> {
    int ontimerInterval;

    public TickSource(int ontimerInterval) {
        this.ontimerInterval = ontimerInterval;
    }

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (running) {
            System.out.println(ontimerInterval);
            Thread.sleep(ontimerInterval);
            ctx.collect("tick");
        }
    }


    @Override
    public void cancel() {
        running = false;
    }
}
