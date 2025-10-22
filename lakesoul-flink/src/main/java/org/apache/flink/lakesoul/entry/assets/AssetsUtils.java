//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.lakesoul.entry.assets;

public class AssetsUtils {
    public AssetsUtils() {
    }

    public static String[] parseFileOpsString(String fileOPs) {
        String[] fileInfo = new String[]{fileOPs.split(",")[1], fileOPs.split(",")[2]};
        return fileInfo;
    }
}
