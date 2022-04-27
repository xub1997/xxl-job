package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.cron.CronExpression;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.scheduler.MisfireStrategyEnum;
import com.xxl.job.admin.core.scheduler.ScheduleTypeEnum;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author xuxueli 2019-05-21
 */
public class JobScheduleHelper {
    private static Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);

    private static JobScheduleHelper instance = new JobScheduleHelper();

    public static JobScheduleHelper getInstance() {
        return instance;
    }

    //预读的毫秒数
    public static final long PRE_READ_MS = 5000;    // pre read

    //预读和调度过期任务的线程
    private Thread scheduleThread;
    private Thread ringThread;
    private volatile boolean scheduleThreadToStop = false;
    private volatile boolean ringThreadToStop = false;
    /**
     * 按时刻（秒）调度 job
     */
    private volatile static Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();

    public void start() {

        /**
         * 1、如果使用线程调度 Job，存在的第一个问题是：如果某个 Job 在调度时比较耗时，就可能阻塞后续的 Job，导致后续 job 的执行有延迟，怎么解决这个问题？
         * 在前面 JobTriggerPoolHelper 我们已经知道，admin 在调度 job 时是 ”使用线程池、线程“ 异步执行调度任务，避免了主线程的阻塞。
         *
         * 2、使用线程定时调度 job，存在的第二个问题是：怎么保证 job 在指定的时间执行，而不会出现大量延迟？
         * admin 使用 ”预读“ 的方式，提前读取在未来一段时间内要执行的 job，提前取到内存中，并使用 “时间轮算法” 按时间分组 job，把未来要执行的 job 下一个时间段执行。
         *
         * 3、还隐藏第三个问题：admin 服务是可以多实例部署的，在这种情况下该怎么避免一个 job 被多个实例重复调度？
         * admin 把一张数据表作为 “分布式锁” 来保证只有一个 admin 实例能执行 job 调度，又通过随机 sleep 线程一段时间，来降低线程之间的竞争。
         *
         */

        // schedule thread
        scheduleThread = new Thread(new Runnable() {
            @Override
            public void run() {

                /**
                 * 为了降低锁竞争，在线程开始前会先 sleep 4000～5000 毫秒的随机值（不能大于 5000 毫秒，5000 毫秒是预读的时间范围）；
                 */
                try {
                    TimeUnit.MILLISECONDS.sleep(5000 - System.currentTimeMillis() % 1000);
                } catch (InterruptedException e) {
                    if (!scheduleThreadToStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>> init xxl-job admin scheduler success.");

                /**
                 * 通过预读，一方面会把过期一小段时间的 job 执行一遍，
                 * 另一方面会把未来一小段时间内要执行的 job 取出，
                 * 保存进一个 map 对象 ringData 中，等待另一个线程调度。
                 * 这样就避免了某些 job 到了时间还没执行
                 */

                // pre-read count: treadpool-size * trigger-qps (each trigger cost 50ms, qps = 1000/50 = 20)
                // 预读数量 = 触发器线程池数量 * 触发器的qps速度 （每一个触发器触发耗费50ms，qps = 1000/50=20）
                //预读数量 =（快速触发器线程池数量 + 低速触发器线程池数量）* 20
                int preReadCount = (XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax() + XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax()) * 20;

                while (!scheduleThreadToStop) {

                    // Scan Job
                    long start = System.currentTimeMillis();

                    Connection conn = null;
                    Boolean connAutoCommit = null;
                    PreparedStatement preparedStatement = null;

                    //是否预读到数据
                    boolean preReadSuc = true;
                    try {

                        conn = XxlJobAdminConfig.getAdminConfig().getDataSource().getConnection();
                        connAutoCommit = conn.getAutoCommit();
                        conn.setAutoCommit(false);

                        //获取数据库链接，通过 SELECT FOR UPDATE 来尝试获取 X锁
                        //基于数据库的分布式锁，“select * from xxl_job_lock where lock_name = ‘schedule_lock’ for update”
                        preparedStatement = conn.prepareStatement("select * from xxl_job_lock where lock_name = 'schedule_lock' for update");
                        preparedStatement.execute();

                        // tx start

                        /**
                         * 1、预读出 “下次执行时间 <= now + 5000 毫秒内” 的部分 job,
                         * 然后把 job 添加到一个 map 对象 ringData 中，然后让另一个线程从该 map 对象中取出，再次调度
                         */

                        // 1、pre read
                        long nowTime = System.currentTimeMillis();


                        /**
                         * triggerStatus;		// 调度状态：0-停止，1-运行
                         * 预读出 “下次执行时间 <= now + 5000 毫秒内” 的部分 job
                         * SELECT <include refid="Base_Column_List" />
                         * 		FROM xxl_job_info AS t
                         * 		WHERE t.trigger_status = 1
                         * 			and t.trigger_next_time <![CDATA[ <= ]]> #{maxNextTime}
                         * 		ORDER BY id ASC
                         * 		LIMIT #{pagesize}
                         */
                        List<XxlJobInfo> scheduleList = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);

                        if (scheduleList != null && scheduleList.size() > 0) {
                            // 2、push time-ring  添加到一个 map 对象 ringData
                            //根据它们下一次执行时间划分成三段，执行三种不同的逻辑。
                            for (XxlJobInfo jobInfo : scheduleList) {

                                // time-ring jump
                                if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS) {
                                    //下次执行时间在(−∞,now−5000) 范围内,说明过期时间已经大于 5000 毫秒，这时如果过期策略要求调度，就调度一次
                                    // 2.1、trigger-expire > 5s：pass && make next-trigger-time
                                    logger.warn(">>>>>>>>>>> xxl-job, schedule misfire, jobId = " + jobInfo.getId());

                                    // 1、misfire match
                                    MisfireStrategyEnum misfireStrategyEnum = MisfireStrategyEnum.match(jobInfo.getMisfireStrategy(), MisfireStrategyEnum.DO_NOTHING);
                                    if (MisfireStrategyEnum.FIRE_ONCE_NOW == misfireStrategyEnum) {
                                        // FIRE_ONCE_NOW 》 trigger
                                        JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.MISFIRE, -1, null, null, null);
                                        logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId());
                                    }

                                    // 2、fresh next 更新下一次执行时间
                                    refreshNextValidTime(jobInfo, new Date());

                                } else if (nowTime > jobInfo.getTriggerNextTime()) {
                                    //下次执行时间在 [now - 5000, now) 范围内,说明过期时间小于5000毫秒，只能算是延迟不能算是过期，直接调度一次
                                    // 2.2、trigger-expire < 5s：direct-trigger && make next-trigger-time

                                    // 1、trigger 调度一次
                                    JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1, null, null, null);
                                    logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId());

                                    // 2、fresh next 更新下一次调度时间
                                    refreshNextValidTime(jobInfo, new Date());

                                    //如果当前 job 处于 ”可以被调度“ 的状态，且下一次执行时间在 5000 毫秒内，就记录下 job Id，等待后面轮询
                                    // next-trigger-time in 5s, pre-read again
                                    if (jobInfo.getTriggerStatus() == 1 && nowTime + PRE_READ_MS > jobInfo.getTriggerNextTime()) {

                                        // 1、make ring second 下次调度的时刻：秒
                                        int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);

                                        // 2、push time ring 保存进 ringData 中
                                        pushTimeRing(ringSecond, jobInfo.getId());

                                        // 3、fresh next 刷新下一次的调度时间
                                        refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));

                                    }

                                } else {
                                    /**
                                     * 下次执行时间在 [now, now + 5000)[now,now+5000) 范围内,
                                     * 说明还没到执行时间，为了省下下次预读的 IO 耗时，这里会记录下 job id，等待后面的调度
                                     */

                                    // 2.3、trigger-pre-read：time-ring trigger && make next-trigger-time

                                    // 1、make ring second 算出时间轮位置
                                    int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);

                                    // 2、push time ring 保存进 ringData 中
                                    pushTimeRing(ringSecond, jobInfo.getId());

                                    // 3、fresh next 刷新下一次的调度时间
                                    refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));

                                }

                            }

                            // 3、update trigger info 更新作业信息：上面的3个步骤结束后，会更新 jobInfo 的 trigger_last_time、trigger_next_time、trigger_status 字段
                            for (XxlJobInfo jobInfo : scheduleList) {
                                XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleUpdate(jobInfo);
                            }

                        } else {
                            preReadSuc = false;
                        }

                        // tx stop


                    } catch (Exception e) {
                        if (!scheduleThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread error:{}", e);
                        }
                    } finally {

                        // commit
                        if (conn != null) {
                            try {
                                conn.commit();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.setAutoCommit(connAutoCommit);
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }

                        // close PreparedStatement
                        if (null != preparedStatement) {
                            try {
                                preparedStatement.close();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }
                    }
                    long cost = System.currentTimeMillis() - start;


                    /**
                     * 在线程结束当前循环时，会根据耗时和是否有预读数据，选择不同的 sleep 策略：
                     *
                     * 耗时超过1000 毫秒，不sleep，直接开始下一次循环；
                     * 耗时小于1000 毫秒，根据是否有预读数据，sleep 一个大小不同的随机时长：
                     * preReadSuc：是否预读数据
                     * 有预读数据，sleep 时间短一些，在 0～1000 毫秒范围内；
                     * 没有预读数据，sleep 时间长一些，在 0～4000 毫秒范围内；
                     *
                     */
                    // Wait seconds, align second
                    if (cost < 1000) {  // scan-overtime, not wait
                        try {
                            // pre-read period: success > scan each second; fail > skip this period;
                            TimeUnit.MILLISECONDS.sleep((preReadSuc ? 1000 : PRE_READ_MS) - System.currentTimeMillis() % 1000);
                        } catch (InterruptedException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }

                }

                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
            }
        });
        scheduleThread.setDaemon(true);
        scheduleThread.setName("xxl-job, admin JobScheduleHelper#scheduleThread");
        scheduleThread.start();


        // ring thread
        ringThread = new Thread(new Runnable() {
            @Override
            public void run() {

                while (!ringThreadToStop) {

                    /**
                     * 在执行轮询调度前，有一个时间在 0～1000 毫秒范围内的 sleep。如果没有这个 sleep，该线程会一直执行，
                     * 而 ringData 中当前时刻（秒）的数据可能已经为空，会导致大量无效的操作；
                     * 增加了这个 sleep 之后，可以避免这种无效的操作。
                     * 之所以 sleep 时间在 1000 毫秒以内，是因为调度时刻最小精确到秒，一秒的 sleep 可以避免 job 的延迟。
                     *
                     */
                    // align second
                    try {
                        TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
                    } catch (InterruptedException e) {
                        if (!ringThreadToStop) {
                            logger.error(e.getMessage(), e);
                        }
                    }

                    try {
                        // second data
                        List<Integer> ringItemData = new ArrayList<>();
                        int nowSecond = Calendar.getInstance().get(Calendar.SECOND);
                        // 避免处理耗时太长，跨过刻度，向前校验一个刻度；
                        //每次轮询调度时，只取出当前时刻（秒）、前一秒内的 job，不会去调度与现在相隔太久的 job
                        for (int i = 0; i < 2; i++) {
                            List<Integer> tmpData = ringData.remove((nowSecond + 60 - i) % 60);
                            if (tmpData != null) {
                                ringItemData.addAll(tmpData);
                            }
                        }

                        // ring trigger 遍历 job Id，执行调度
                        logger.debug(">>>>>>>>>>> xxl-job, time-ring beat : " + nowSecond + " = " + Arrays.asList(ringItemData));
                        if (ringItemData.size() > 0) {
                            // do trigger
                            for (int jobId : ringItemData) {
                                // do trigger
                                JobTriggerPoolHelper.trigger(jobId, TriggerTypeEnum.CRON, -1, null, null, null);
                            }
                            // clear
                            ringItemData.clear();
                        }
                    } catch (Exception e) {
                        if (!ringThreadToStop) {
                            logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error:{}", e);
                        }
                    }
                }
                logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
            }
        });
        ringThread.setDaemon(true);
        ringThread.setName("xxl-job, admin JobScheduleHelper#ringThread");
        ringThread.start();
    }

    private void refreshNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
        Date nextValidTime = generateNextValidTime(jobInfo, fromTime);
        if (nextValidTime != null) {
            jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
            jobInfo.setTriggerNextTime(nextValidTime.getTime());
        } else {
            //triggerStatus;		// 调度状态：0-停止，1-运行
            jobInfo.setTriggerStatus(0);
            jobInfo.setTriggerLastTime(0);
            jobInfo.setTriggerNextTime(0);
            logger.warn(">>>>>>>>>>> xxl-job, refreshNextValidTime fail for job: jobId={}, scheduleType={}, scheduleConf={}",
                    jobInfo.getId(), jobInfo.getScheduleType(), jobInfo.getScheduleConf());
        }
    }

    private void pushTimeRing(int ringSecond, int jobId) {
        // push async ring
        List<Integer> ringItemData = ringData.get(ringSecond);
        if (ringItemData == null) {
            ringItemData = new ArrayList<Integer>();
            ringData.put(ringSecond, ringItemData);
        }
        ringItemData.add(jobId);

        logger.debug(">>>>>>>>>>> xxl-job, schedule push time-ring : " + ringSecond + " = " + Arrays.asList(ringItemData));
    }

    public void toStop() {

        // 1、stop schedule
        scheduleThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);  // wait  给线程 1s 的时间去执行任务
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        //如果线程不是终止状态，就让它执行完所有任务
        if (scheduleThread.getState() != Thread.State.TERMINATED) {
            // interrupt and wait
            scheduleThread.interrupt();
            try {
                scheduleThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // if has ring data 如果时间轮上面还有数据就休眠8s
        boolean hasRingData = false;
        if (!ringData.isEmpty()) {
            for (int second : ringData.keySet()) {
                List<Integer> tmpData = ringData.get(second);
                if (tmpData != null && tmpData.size() > 0) {
                    hasRingData = true;
                    break;
                }
            }
        }
        if (hasRingData) {
            try {
                TimeUnit.SECONDS.sleep(8);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // stop ring (wait job-in-memory stop) 停止时间轮线程，并给线程 1s 的时间去执行任务
        ringThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        //如果线程不是终止状态，就让它执行完所有任务
        if (ringThread.getState() != Thread.State.TERMINATED) {
            // interrupt and wait
            ringThread.interrupt();
            try {
                ringThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper stop");
    }


    // ---------------------- tools ----------------------
    public static Date generateNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
        ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
        if (ScheduleTypeEnum.CRON == scheduleTypeEnum) {
            Date nextValidTime = new CronExpression(jobInfo.getScheduleConf()).getNextValidTimeAfter(fromTime);
            return nextValidTime;
        } else if (ScheduleTypeEnum.FIX_RATE == scheduleTypeEnum /*|| ScheduleTypeEnum.FIX_DELAY == scheduleTypeEnum*/) {
            return new Date(fromTime.getTime() + Integer.valueOf(jobInfo.getScheduleConf()) * 1000);
        }
        return null;
    }

}
