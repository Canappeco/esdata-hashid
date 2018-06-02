package cn.faceall.es;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.transport.TransportClient;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 *
 * nohup java -Xms4g -Xmx4g -classpath ./target/ESDATA-0.0.1-SNAPSHOT-jar-with-dependencies.jar cn.faceall.es.InsertHashData >> ./5y-6-0.log 2>&1 &
 */

public class InsertHashData {

    public static void main(String[] args) {
        try {
            //这里设置线程数
            ThreadPoolExecutor es = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
            // es.shutdown(); //
            // 如果先关闭再执行任务，则会拒绝执行任务，抛出RejectedExecutionException异常
            int step = 250000;
            for (long i = 0; i < 20; i++) {
                es.execute(new InsertHashTask(0, 250000, i));
            }
            //等待线程完成
            es.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static boolean isExistsIndex(String index, TransportClient client){
        IndicesExistsResponse response =
                client.admin().indices().prepareExists(index).execute().actionGet();
        return response.isExists();
    }
}
