package org.voltdb.voltdbedge;

public interface LogicalDeviceUpStreamIFace {

    public long deviceNeedsAttention();

    public long powerCycled();

}
