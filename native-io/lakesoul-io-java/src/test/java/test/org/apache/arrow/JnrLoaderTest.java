package test.org.apache.arrow;

import org.apache.arrow.lakesoul.io.jnr.JnrLoader;
import org.junit.Assert;
import org.junit.Test;

public class JnrLoaderTest {
    @Test
    public void loadLibrary() {
        Assert.assertNotNull(JnrLoader.get());
    }
}
