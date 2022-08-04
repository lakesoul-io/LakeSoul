import org.bytedeco.javacpp.annotation.Platform;
import org.bytedeco.javacpp.annotation.Properties;
import org.bytedeco.javacpp.tools.InfoMap;
import org.bytedeco.javacpp.tools.InfoMapper;

@Properties(
        target = "CDataJavaToCppExample",
        value = @Platform(
                include = {
                        "CDataCppBridge.h"
                },
                compiler = {"cpp11"},
                linkpath = {"/Users/ceng/Documents/GitHub/arrow/cpp/build/debug"},
                includepath = {"/Users/ceng/Documents/GitHub/arrow/cpp/build/src", "/Users/ceng/Documents/GitHub/arrow/cpp/src"},
                link = {"arrow"}
        )
)
public class CDataJavaConfig implements InfoMapper {

    @Override
    public void map(InfoMap infoMap) {
    }

}
