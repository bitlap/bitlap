package org.bitlap.common;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 
 *  @author
 *    梦境迷离
 *  @version 1.0,2023/3/18
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PACKAGE)
public @interface BitlapVersionAnnotation {

    String version();

    String shortVersion();

    String user();

    String date();

    String url();

    String revision();

    String srcChecksum();

}
