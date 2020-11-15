/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 */
package org.bitlap.common.bitmap.rbm.buffer;

import org.bitlap.common.bitmap.rbm.BitmapDataProvider;
import org.bitlap.common.bitmap.rbm.BitmapDataProviderSupplier;

/**
 * A {@link BitmapDataProviderSupplier} providing {@link MutableRoaringBitmap} as
 * {@link BitmapDataProvider}
 * 
 * @author Benoit Lacelle
 *
 */
public class MutableRoaringBitmapSupplier implements BitmapDataProviderSupplier {

  @Override
  public BitmapDataProvider newEmpty() {
    return new MutableRoaringBitmap();
  }

}
