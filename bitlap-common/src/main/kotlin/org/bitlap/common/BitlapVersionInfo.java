package org.bitlap.common;

/**
 * @author 梦境迷离
 * @version 1.0, 2023/3/18
 */
public class BitlapVersionInfo {
    private static Package myPackage;
    private static BitlapVersionAnnotation version;

    static {
        myPackage = BitlapVersionAnnotation.class.getPackage();
        version = myPackage.getAnnotation(BitlapVersionAnnotation.class);
    }

    static Package getPackage() {
        return myPackage;
    }

    public static String getVersion() {
        return version != null ? version.version() : "Unknown";
    }

    public static String getShortVersion() {
        return version != null ? version.shortVersion() : "Unknown";
    }

    public static String getRevision() {
        return version != null ? version.revision() : "Unknown";
    }

    public static String getDate() {
        return version != null ? version.date() : "Unknown";
    }

    public static String getUser() {
        return version != null ? version.user() : "Unknown";
    }

    public static String getUrl() {
        return version != null ? version.url() : "Unknown";
    }

    public static String getSrcChecksum() {
        return version != null ? version.srcChecksum() : "Unknown";
    }

    public static String getBuildVersion() {
        return BitlapVersionInfo.getVersion() + " from " + BitlapVersionInfo.getRevision() + " by " + BitlapVersionInfo.getUser() + " source checksum " + BitlapVersionInfo.getSrcChecksum();
    }

}
