package cn.faceall.es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 将所有hashid索引删除
 * nohup java -classpath ./target/ESDATA-0.0.1-SNAPSHOT-jar-with-dependencies.jar cn.faceall.es.DeleteIndex >> ./5y-6-0.log 2>&1 &
 * curl -XDELETE '192.168.1.140:9200/test_all?pretty'
 *
 */

public class DeleteIndex {

    public static void main(String[] args) {

        TransportClient client;
        String indexNameAll = "test_all";
        String cluster_name = "face_all_es";

        List<String> allIPs = new ArrayList<String>();
        allIPs.add("192.168.1.140");
        allIPs.add("192.168.1.174");
        allIPs.add("192.168.1.231");
        Collections.shuffle(allIPs);

        Settings settings = Settings.builder()
                .put("cluster.name", cluster_name)
                .put("client.transport.sniff", true)
                .build();

        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(allIPs.get(0)), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(allIPs.get(1)), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(allIPs.get(2)), 9300));
            //删除test_all大索引
            client.admin().indices().prepareDelete(indexNameAll).execute().actionGet();
            for (int i = 0; i < 2000; i++) {
                try{
                    client.admin().indices().prepareDelete(i+"").execute().actionGet();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("delete index "+i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
