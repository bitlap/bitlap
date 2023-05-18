package org.bitlap;

import org.bitlap.jdbc.BitlapDriver;

/**
 * @author 梦境迷离
 * @version 1.0, 2023/5/3
 */
public class Driver extends BitlapDriver {
    static {
        new org.bitlap.Driver().register();
    }
}
