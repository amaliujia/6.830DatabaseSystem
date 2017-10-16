package simpledb;

import org.junit.Test;

public class myTest {
    @Test
    public void testStringArrayDefaultValue() {
        String[] strings = new String[1];
        assert (strings[0] == null);
    }
}
