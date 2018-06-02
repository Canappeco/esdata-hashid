package cn.faceall.es;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 删除数据
 */

public class DeleteTestData {

    public static void main(String[] args) {

        try {
            //这里设置线程数
            ThreadPoolExecutor es = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
            // es.shutdown(); //
            // 如果先关闭再执行任务，则会拒绝执行任务，抛出RejectedExecutionException异常
            int step = 25000;
            for (int i = 0; i < 500000; i+=step){
                es.execute(new DeleteTask(i, i+step));
            }
            //等待线程完成
            es.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
