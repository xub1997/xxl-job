package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.trigger.XxlJobTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * job trigger thread pool helper
 *
 * @author xuxueli 2018-07-03 21:08:07
 */
public class JobTriggerPoolHelper {
    private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);


    // ---------------------- trigger pool ----------------------

    // fast/slow thread pool
    private ThreadPoolExecutor fastTriggerPool = null;
    private ThreadPoolExecutor slowTriggerPool = null;

    public void start(){
        fastTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(1000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode());
                    }
                });

        slowTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode());
                    }
                });
    }


    public void stop() {
        //triggerPool.shutdown();
        fastTriggerPool.shutdownNow();
        slowTriggerPool.shutdownNow();
        logger.info(">>>>>>>>> xxl-job trigger thread pool shutdown success.");
    }


    //// 属性变量，初始值等于 JobTriggerPoolHelper 对象构造时的分钟数
    //// 每次调用 XxlJobTrigger.trigger() 方法时，值等于上一次调用的分钟数
    // job timeout count
    private volatile long minTim = System.currentTimeMillis()/60000;     // ms > min
    private volatile ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();


    /**
     * add trigger
     */
    public void addTrigger(final int jobId,
                           final TriggerTypeEnum triggerType,
                           final int failRetryCount,
                           final String executorShardingParam,
                           final String executorParam,
                           final String addressList) {
        //不同 job 存在执行时长的差异，为了避免不同耗时 job 之间相互阻塞，xxl-job 根据 job 的响应时间，对 job 进行了区分，主要体现在：
        //
        //如果 job 耗时短，就在 fastTriggerPool 线程池中创建线程；
        //如果 job 耗时长且调用频繁，就在 slowTriggerPool 线程池中创建线程；

        // choose thread pool 选择线程池，如果在一分钟内调度超过10次，使用 slowTriggerPool
        ThreadPoolExecutor triggerPool_ = fastTriggerPool;
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        if (jobTimeoutCount!=null && jobTimeoutCount.get() > 10) {      // job-timeout 10 times in 1 min
            triggerPool_ = slowTriggerPool;
        }

        // trigger
        triggerPool_.execute(new Runnable() {
            @Override
            public void run() {

                // 开始调用 XxlJobTrigger.trigger() 的时间
                long start = System.currentTimeMillis();

                try {
                    // do trigger 触发器触发作业
                    XxlJobTrigger.trigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                } finally {

                    //调用 XxlJobTrigger.trigger() 方法后，根据两个值来更新 jobTimeoutCountMap 的值：
                    //
                    //当前时间与上次调用是否在一分钟以内，如果不在一分钟以内，就清空 map；
                    //本次 XxlJobTrigger.trigger() 的调用是否超过 500 毫秒，如果超过 500 毫秒，就在 map 中增加 job_id 的超时次数；

                    // 当前时间的分钟数，如果和前一次调用不在同一分钟内，就清空 jobTimeoutCountMap
                    // check timeout-count-map
                    long minTim_now = System.currentTimeMillis()/60000;
                    if (minTim != minTim_now) {
                        minTim = minTim_now;
                        //admin 服务发起 job 调度请求时，是在静态方法 public static void trigger() 中
                        // 调用静态变量 private static JobTriggerPoolHelper helper 的 addTrigger 方法来发起请求的。
                        // minTim 和 jobTimeoutCountMap 虽然不是 static 修饰的，但可以看做是全局唯一的（因为持有它们的对象是全局唯一的）
                        // 因此这两个参数维护的是 admin 服务全局的调度时间和超时次数，为了避免记录的数据量过大，需要每分钟清空一次数据的操作。

                        jobTimeoutCountMap.clear();
                    }

                    // 如果用时超过 500 毫秒，就增加一次它的慢调用次数
                    // incr timeout-count-map
                    long cost = System.currentTimeMillis()-start;
                    if (cost > 500) {       // ob-timeout threshold 500ms
                        AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                        if (timeoutCount != null) {
                            timeoutCount.incrementAndGet();
                        }
                    }

                }

            }
        });
    }



    // ---------------------- helper ----------------------

    private static JobTriggerPoolHelper helper = new JobTriggerPoolHelper();

    public static void toStart() {
        helper.start();
    }
    public static void toStop() {
        helper.stop();
    }

    /**
     * @param jobId
     * @param triggerType
     * @param failRetryCount
     * 			>=0: use this param
     * 			<0: use param from job info config
     * @param executorShardingParam
     * @param executorParam
     *          null: use job param
     *          not null: cover job param
     */
    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam, String executorParam, String addressList) {
        //调用时，根据运行时间选择对应的线程池
        helper.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
    }

}
