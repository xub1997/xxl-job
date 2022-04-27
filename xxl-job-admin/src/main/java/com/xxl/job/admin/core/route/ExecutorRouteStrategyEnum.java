package com.xxl.job.admin.core.route;

import com.xxl.job.admin.core.route.strategy.*;
import com.xxl.job.admin.core.util.I18nUtil;

/**
 * Created by xuxueli on 17/3/10.
 */
public enum ExecutorRouteStrategyEnum {

    /**
     * 第一个
     */
    FIRST(I18nUtil.getString("jobconf_route_first"), new ExecutorRouteFirst()),
    /**
     * 最后一个
     */
    LAST(I18nUtil.getString("jobconf_route_last"), new ExecutorRouteLast()),
    /**
     * 轮询（根据jobId递增，并对执行器地址大小进行求余，实现轮训）
     */
    ROUND(I18nUtil.getString("jobconf_route_round"), new ExecutorRouteRound()),
    /**
     * 随机
     */
    RANDOM(I18nUtil.getString("jobconf_route_random"), new ExecutorRouteRandom()),
    /**
     * 一致性HASH( * 分组下机器地址相同，不同JOB均匀散列在不同机器上，保证分组下机器分配JOB平均；且每个JOB固定调度其中一台机器；
     *  *      a、virtual node：解决不均衡问题
     *  *      b、hash method replace hashCode：String的hashCode可能重复，需要进一步扩大hashCode的取值范围)
     */
    CONSISTENT_HASH(I18nUtil.getString("jobconf_route_consistenthash"), new ExecutorRouteConsistentHash()),
    /**
     * 最不经常使用
     */
    LEAST_FREQUENTLY_USED(I18nUtil.getString("jobconf_route_lfu"), new ExecutorRouteLFU()),
    /**
     * 最近最久未使用（使用LinkedHashMap实现LRU）
     *  a、accessOrder：true=访问顺序排序（get/put时排序）；false=插入顺序排期；
     *  b、removeEldestEntry：新增元素时将会调用，返回true时会删除最老元素；可封装LinkedHashMap并重写该方法，比如定义最大容量，超出是返回true即可实现固定长度的LRU算法；
     */
    LEAST_RECENTLY_USED(I18nUtil.getString("jobconf_route_lru"), new ExecutorRouteLRU()),
    /**
     * 故障转移（通过调用心跳接口，判断执行器地址是否可用，返回其中一个可用的地址）
     */
    FAILOVER(I18nUtil.getString("jobconf_route_failover"), new ExecutorRouteFailover()),
    /**
     * 忙碌转移(通过调用空闲心跳，判断作业作业在对应执行器地址上是否正在运行或存在待运行作业，返回其中一个空闲的执行器地址)
     */
    BUSYOVER(I18nUtil.getString("jobconf_route_busyover"), new ExecutorRouteBusyover()),
    /**
     * 分片广播
     */
    SHARDING_BROADCAST(I18nUtil.getString("jobconf_route_shard"), null);

    ExecutorRouteStrategyEnum(String title, ExecutorRouter router) {
        this.title = title;
        this.router = router;
    }

    private String title;
    private ExecutorRouter router;

    public String getTitle() {
        return title;
    }
    public ExecutorRouter getRouter() {
        return router;
    }

    public static ExecutorRouteStrategyEnum match(String name, ExecutorRouteStrategyEnum defaultItem){
        if (name != null) {
            for (ExecutorRouteStrategyEnum item: ExecutorRouteStrategyEnum.values()) {
                if (item.name().equals(name)) {
                    return item;
                }
            }
        }
        return defaultItem;
    }

}
