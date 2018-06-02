package cn.faceall.es;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 单独对一个index插入feature数据
 * 执行命令：
 * nohup java -classpath ./target/esdata-1.0-SNAPSHOT-jar-with-dependencies.jar cn.faceall.es.InsertSingleHashData >> ./output.log 2>&1 &
 */

public class InsertSingleHashData {

    public static void main(String[] args) {
        try {
            //这里设置线程数
            ThreadPoolExecutor es = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
            // es.shutdown(); //
            // 如果先关闭再执行任务，则会拒绝执行任务，抛出RejectedExecutionException异常
            for (long i = 0; i < 200; i++) {
                es.execute(new InsertSingleHashTask(0, 500000));
            }
            //等待线程完成
            es.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
