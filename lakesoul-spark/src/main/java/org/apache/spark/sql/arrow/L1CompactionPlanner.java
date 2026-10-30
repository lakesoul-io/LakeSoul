// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.arrow;

import java.util.ArrayList;
import java.util.List;

/** Plans disjoint, ordered intervals. Singleton intervals are retained without IO. */
final class L1CompactionPlanner {
    static final class Group {
        final int start;
        final int end;
        final long bytes;

        Group(int start, int end, long bytes) {
            this.start = start;
            this.end = end;
            this.bytes = bytes;
        }

        int count() { return end - start; }
    }

    static List<Group> plan(List<Long> sizes, long target, long maxBytes, int maxFiles,
            int smallFileLimit, boolean pressure, int pressureFiles, long pressureBytes) {
        if (target <= 0 || maxBytes < target || maxFiles < 2 || smallFileLimit < 2
                || pressureFiles < 2 || pressureBytes <= 0) {
            throw new IllegalArgumentException("Invalid L1 compaction planning limits");
        }
        int smallCount = 0;
        for (long size : sizes) {
            if (size < 0) throw new IllegalArgumentException("Negative file size");
            if (size < target) smallCount++;
        }
        List<Group> groups = new ArrayList<>();
        int i = 0;
        while (i < sizes.size()) {
            int start = i;
            long bytes = sizes.get(i++);
            // Oversized singletons are retained, never submitted to a reader.
            if (bytes <= maxBytes && (pressure || bytes < target)) {
                while (i < sizes.size()) {
                    if (pressure ? (i - start >= pressureFiles || bytes >= pressureBytes)
                            : bytes >= target) break;
                    long next = sizes.get(i);
                    if ((!pressure && next >= target) || i - start >= maxFiles
                            || next > maxBytes - bytes) break;
                    bytes += next;
                    i++;
                }
            }
            groups.add(new Group(start, i, bytes));
        }
        // Absorb a small tail before IO. In regular mode, do not cross a large file boundary.
        for (int g = 1; g < groups.size();) {
            Group left = groups.get(g - 1), right = groups.get(g);
            boolean boundary = !pressure && ((left.count() == 1 && left.bytes >= target)
                    || (right.count() == 1 && right.bytes >= target));
            if (!boundary && right.bytes < target && fits(left, right, maxBytes, maxFiles)) {
                join(groups, g - 1, g);
            } else g++;
        }
        if (smallCount >= smallFileLimit) {
            connectSmallGroups(groups, target, maxBytes, maxFiles);
            groups = absorbAdjacentLargeGroups(groups, target, maxBytes, maxFiles);
        }
        return groups;
    }

    /** Recompute small input blocks after each connection; no files are written here. */
    private static void connectSmallGroups(
            List<Group> groups, long target, long maxBytes, int maxFiles) {
        while (true) {
            List<Integer> small = smallGroups(groups, target);
            if (small.size() < 2) return;
            int bestStart = -1, bestEnd = -1;
            long bestCost = Long.MAX_VALUE;
            for (int s = 1; s < small.size(); s++) {
                int from = small.get(s - 1), to = small.get(s);
                long bytes = rangeBytes(groups, from, to, maxBytes);
                if (bytes >= 0 && groups.get(to).end - groups.get(from).start <= maxFiles) {
                    long cost = bytes - groups.get(from).bytes - groups.get(to).bytes;
                    if (cost < bestCost) {
                        bestStart = from;
                        bestEnd = to;
                        bestCost = cost;
                    }
                }
            }
            if (bestStart < 0) return;
            join(groups, bestStart, bestEnd);
        }
    }

    /** Select disjoint pairs in one scan, then rebuild once. New groups cannot be reselected. */
    private static List<Group> absorbAdjacentLargeGroups(
            List<Group> groups, long target, long maxBytes, int maxFiles) {
        List<Integer> small = smallGroups(groups, target);
        // Do not pull in a large neighbor merely to eliminate one remaining small block.
        if (small.size() < 2) return groups;
        boolean[] occupied = new boolean[groups.size()];
        boolean[] pairStart = new boolean[groups.size()];
        for (int g : small) {
            if (occupied[g]) continue;
            int chosen = -1;
            for (int neighbor : new int[] {g - 1, g + 1}) {
                if (neighbor < 0 || neighbor >= groups.size() || occupied[neighbor]) continue;
                Group other = groups.get(neighbor);
                int from = Math.min(g, neighbor), to = Math.max(g, neighbor);
                if (other.bytes >= target
                        && fits(groups.get(from), groups.get(to), maxBytes, maxFiles)
                        && (chosen < 0 || other.bytes < groups.get(chosen).bytes)) {
                    chosen = neighbor;
                }
            }
            if (chosen >= 0) {
                occupied[g] = true;
                occupied[chosen] = true;
                pairStart[Math.min(g, chosen)] = true;
            }
        }
        List<Group> result = new ArrayList<>();
        for (int g = 0; g < groups.size();) {
            Group left = groups.get(g);
            if (pairStart[g]) {
                Group right = groups.get(g + 1);
                result.add(new Group(left.start, right.end, left.bytes + right.bytes));
                g += 2;
            } else {
                result.add(left);
                g++;
            }
        }
        return result;
    }

    private static List<Integer> smallGroups(List<Group> groups, long target) {
        List<Integer> small = new ArrayList<>();
        for (int g = 0; g < groups.size(); g++) {
            if (groups.get(g).bytes < target) small.add(g);
        }
        return small;
    }

    private static boolean fits(Group left, Group right, long maxBytes, int maxFiles) {
        return left.bytes <= maxBytes && right.bytes <= maxBytes - left.bytes
                && right.end - left.start <= maxFiles;
    }

    private static long rangeBytes(List<Group> groups, int from, int to, long limit) {
        long bytes = 0;
        for (int i = from; i <= to; i++) {
            if (groups.get(i).bytes > limit - bytes) return -1;
            bytes += groups.get(i).bytes;
        }
        return bytes;
    }

    private static void join(List<Group> groups, int from, int to) {
        Group combined = new Group(groups.get(from).start, groups.get(to).end,
                rangeBytes(groups, from, to, Long.MAX_VALUE));
        groups.subList(from, to + 1).clear();
        groups.add(from, combined);
    }
}
