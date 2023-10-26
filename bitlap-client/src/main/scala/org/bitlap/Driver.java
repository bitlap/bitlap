package org.bitlap;

import org.bitlap.jdbc.BitlapDriver;

public class Driver extends BitlapDriver {
    static {
        new org.bitlap.Driver().register();
    }
}
