package org.bitlap.common;

import com.typesafe.config.*;

/**
 * @author 梦境迷离
 * @version 1.0, 2023/3/18
 */
public class BitlapVersionInfo {

    private static String version;

    static {
        Config config = ConfigFactory.load("maven-version.properties");
        version = config.getString("maven.package.version");
    }


    public static String getVersion() {
        return version != null ? version : "Unknown";
    }

}
