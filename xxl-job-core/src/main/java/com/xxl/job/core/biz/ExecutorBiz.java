package com.xxl.job.core.biz;

import com.xxl.job.core.biz.model.*;

/**
 * Created by xuxueli on 17/3/1.
 */
public interface ExecutorBiz {

    /**
     * beat 用于检查心跳；直接返回成功
     * @return
     */
    public ReturnT<String> beat();

    /**
     * idle beat 用途：用于检查忙碌状态；忙碌中（执行任务中，或者队列中有数据）
     *
     * @param idleBeatParam
     * @return
     */
    public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam);

    /**
     * run 用途：用于执行任务
     * @param triggerParam
     * @return
     */
    public ReturnT<String> run(TriggerParam triggerParam);

    /**
     * kill 用途：用于中断线程
     * @param killParam
     * @return
     */
    public ReturnT<String> kill(KillParam killParam);

    /**
     * log 用途：用于读取日志
     * @param logParam
     * @return
     */
    public ReturnT<LogResult> log(LogParam logParam);

}
