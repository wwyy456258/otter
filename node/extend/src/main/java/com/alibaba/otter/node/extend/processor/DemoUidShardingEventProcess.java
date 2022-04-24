package com.alibaba.otter.node.extend.processor;

/**
 * @author wangyi
 */
public class DemoUidShardingEventProcess extends AbstractUidShardingEventProcessor{
    @Override
    public int getCurrentDbIndex() {
        return 2;
    }

    @Override
    public int getCurrentTableIndex() {
        return 0;
    }
}
