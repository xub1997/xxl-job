package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.thread.*;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.client.ExecutorBizClient;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xuxueli 2018-10-28 00:18:17
 */

public class XxlJobScheduler  {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);


    public void init() throws Exception {
        // init i18n 初始化国际化文件
        initI18n();

        // admin trigger pool start 启动xxl-job触发器线程池（初始化2个线程池：fastTriggerPool、slowTriggerPool），
        // 利用private volatile ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>()统计触发频率 --->根据计算出作业的触发频率分发到fastTriggerPool、slowTriggerPool
        //使用XxlJobTrigger.trigger触发作业
        JobTriggerPoolHelper.toStart();

        // admin registry monitor run 启动xxl-job监听注册线程池（注册 & 更新客户端执行器地址，移除客户端执行器地址）；启动守护线程监听长时间未进行更新的客户端地址进行移除
        JobRegistryHelper.getInstance().start();

        // admin fail-monitor run 启动xxl-job作业执行失败守护线程（任务失败监控，进行任务重试）
        //一个监视线程，以每10 秒一次的频率运行，对失败的 job 进行重试。如果 job 剩余的重试次数大于0，就会 job 进行重试，并发送告警信息
        JobFailMonitorHelper.getInstance().start();

        // admin lose-monitor run ( depend on JobTriggerPoolHelper 依赖xxl-job触发器线程池) 启动 丢失 job 监视器线程，
        // 启动callback监听线程池（处理执行器返回的回调信息）；启动守护线程进行任务结果丢失处理：调度记录停留在 "运行中" 状态超过10min，且对应执行器心跳注册失败不在线，则将本地调度主动标记失败；---->Job 完成线程的辅助类
        JobCompleteHelper.getInstance().start();

        // admin log report start 启动执行情况统计报告守护线程 && 数据库job的执行log清理---->日志统计辅助类
        JobLogReportHelper.getInstance().start();

        // start-schedule  ( depend on JobTriggerPoolHelper 依赖xxl-job触发器线程池) 启动调度守护线程,定时调度 job
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init xxl-job admin success.");
    }

    
    public void destroy() throws Exception {

        //与上面相反

        // stop-schedule 销毁 调度线程
        JobScheduleHelper.getInstance().toStop();

        // admin log report stop 销毁 日志统计和清理线程
        JobLogReportHelper.getInstance().toStop();

        // admin lose-monitor stop 销毁 丢失 job 监视器线程
        JobCompleteHelper.getInstance().toStop();

        // admin fail-monitor stop  销毁 失败 job 监视器线程
        JobFailMonitorHelper.getInstance().toStop();

        // admin registry stop 销毁 注册监听线程
        JobRegistryHelper.getInstance().toStop();

        // admin trigger pool stop 销毁 触发器线程池
        JobTriggerPoolHelper.toStop();

    }

    // ---------------------- I18n ----------------------

    private void initI18n(){
        for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<String, ExecutorBiz>();

    /**
     * 在触发器传入地址去获取执行器调用的时候进行新建ExecutorBizClient（XxlJobTrigger.runExecutor）
     * 以 “懒加载” 的方式给每个 address 创建一个 ExecutorBiz 对象
     * @param address
     * @return
     * @throws Exception
     */
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // valid
        if (address==null || address.trim().length()==0) {
            return null;
        }

        // load-cache
        address = address.trim();
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        // set-cache
        executorBiz = new ExecutorBizClient(address, XxlJobAdminConfig.getAdminConfig().getAccessToken());

        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}
