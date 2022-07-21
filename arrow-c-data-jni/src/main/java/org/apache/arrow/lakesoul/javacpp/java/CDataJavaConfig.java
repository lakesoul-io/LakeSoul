package org.apache.arrow.lakesoul.javacpp.java;

import org.apache.arrow.ArrowUtils;
import org.bytedeco.javacpp.annotation.Platform;
import org.bytedeco.javacpp.annotation.Properties;
import org.bytedeco.javacpp.tools.InfoMap;
import org.bytedeco.javacpp.tools.InfoMapper;

@Properties(
        target = "org.apache.arrow.lakesoul.javacpp.java.CDataJavaToCppInterface",
        value = @Platform(
                include = {
                        "CDataCppBridge.h"
                },
                compiler = {"cpp11"},
                linkpath = {ArrowUtils.LOCAL_ARROW_DIR + "/cpp/build/debug/"},
                includepath = {ArrowUtils.LOCAL_ARROW_DIR + "/cpp/build/src", ArrowUtils.LOCAL_ARROW_DIR + "/cpp/src", ArrowUtils.LOCAL_BASE_DIR+"/src/main/java/org/apache/arrow/lakesoul/javacpp/cpp"},
                link = {"arrow"}
        )
)
public class CDataJavaConfig implements InfoMapper {

    @Override
    public void map(InfoMap infoMap) {
    }
}
