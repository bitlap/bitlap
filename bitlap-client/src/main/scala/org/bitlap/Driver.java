package org.bitlap;

import org.bitlap.jdbc.BitlapDriver;

/**
 * bitlap 驱动。加载驱动时必须使用此类。
 */
public class Driver extends BitlapDriver {
    static {
        new Driver().register();
    }
}
