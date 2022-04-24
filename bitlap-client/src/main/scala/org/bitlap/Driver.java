package org.bitlap;

import org.bitlap.jdbc.BitlapDriver;

/**
 * Driver wrapper
 */
public class Driver extends BitlapDriver {
    static {
        new Driver().register();
    }
}
