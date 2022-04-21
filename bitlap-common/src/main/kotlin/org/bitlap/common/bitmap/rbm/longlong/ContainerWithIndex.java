package org.bitlap.common.bitmap.rbm.longlong;

import org.bitlap.common.bitmap.rbm.Container;

public class ContainerWithIndex {

  private Container container;
  private long containerIdx;

  public ContainerWithIndex(Container container, long containerIdx) {
    this.container = container;
    this.containerIdx = containerIdx;
  }

  public Container getContainer() {
    return container;
  }

  public long getContainerIdx() {
    return containerIdx;
  }
}
