// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.arrow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/** Standalone: run main with assertions independent of Spark/JNI. Sizes use arbitrary units. */
public class L1CompactionPlannerTest {
    @org.junit.Test
    public void planningPreservesOrderAndBudgets() {
        main(new String[0]);
    }

    private static List<L1CompactionPlanner.Group> plan(boolean pressure, int k, Long... sizes) {
        return L1CompactionPlanner.plan(Arrays.asList(sizes), 100, 256, 20, k, pressure, 5, 600);
    }

    private static void expect(List<L1CompactionPlanner.Group> groups, int... ends) {
        if (groups.size() != ends.length) throw new AssertionError("Unexpected group count");
        int start = 0;
        for (int i = 0; i < ends.length; i++) {
            if (groups.get(i).start != start || groups.get(i).end != ends[i]) {
                throw new AssertionError("Unexpected interval");
            }
            start = ends[i];
        }
    }

    public static void main(String[] args) {
        expect(plan(false, 99, 120L, 1L, 2L, 3L, 150L, 2L, 3L), 1, 4, 5, 7);
        expect(plan(false, 2, 120L, 1L, 2L, 3L, 150L, 2L, 3L), 1, 7);
        expect(plan(false, 2, 120L, 1L, 2L, 3L, 300L, 2L, 3L), 4, 5, 7);
        expect(plan(false, 2, 300L, 1L, 300L, 2L, 300L), 1, 2, 3, 4, 5);
        expect(plan(false, 2, 1L, 2L, 3L), 3);
        expect(plan(false, 2, 1L), 1);
        expect(plan(true, 2, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L), 10);
        expect(plan(true, 2, 200L, 80L), 1, 2);
        // Batch absorption handles both disjoint ends without revisiting generated groups.
        expect(plan(false, 2, 120L, 6L, 300L, 5L, 300L, 4L, 120L), 2, 3, 4, 5, 7);
        // Both small blocks can use the same large neighbor, but it must be selected only once.
        expect(plan(false, 2, 30L, 200L, 30L), 2, 3);
        // A single remaining small block does not trigger large-neighbor absorption.
        expect(plan(false, 2, 120L, 2L, 3L), 1, 3);
        Random random = new Random(42);
        for (int trial = 0; trial < 2000; trial++) {
            List<Long> sizes = new ArrayList<>();
            int fileCount = random.nextInt(60);
            for (int i = 0; i < fileCount; i++) sizes.add((long) random.nextInt(400));
            int cap = 2 + random.nextInt(19);
            List<L1CompactionPlanner.Group> groups =
                    L1CompactionPlanner.plan(sizes, 100, 256, cap, 2, random.nextBoolean(), 5, 600);
            int cursor = 0;
            for (L1CompactionPlanner.Group g : groups) {
                if (g.start != cursor || g.end <= g.start)
                    throw new AssertionError("Order/coverage");
                long bytes = 0;
                for (int i = g.start; i < g.end; i++) bytes += sizes.get(i);
                if (bytes != g.bytes || (g.count() > 1 && (bytes > 256 || g.count() > cap))) {
                    throw new AssertionError("Budget");
                }
                cursor = g.end;
            }
            if (cursor != sizes.size()) throw new AssertionError("Missing files");
        }
        System.out.println("PASS: examples and 2000 randomized order/coverage/budget checks");
    }
}
