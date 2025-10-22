// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.assets;

public class AssetsUtils {
    public AssetsUtils() {
    }

    public static String[] parseFileOpsString(String fileOPs) {
        String[] fileInfo = new String[]{fileOPs.split(",")[1], fileOPs.split(",")[2]};
        return fileInfo;
    }
}
