package org.bitlap.common.bitmap.rbm;

public interface WordStorage<T> {

  T add(char value);

  boolean isEmpty();

  T runOptimize();

}
