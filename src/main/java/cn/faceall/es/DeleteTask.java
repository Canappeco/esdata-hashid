package cn.faceall.es;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */

public class DeleteTask implements Runnable{

    long docStartId;
    long docEndId;
    TransportClient client;
    String cluster_name = "face_all_es";
    String indexNameAll = "test_all";
    String type = "testData";
    List<Long> docIdList = new ArrayList<Long>();
    Map<Long, ArrayList<Long>> IndexMap = new TreeMap<Long, ArrayList<Long>>();

    public DeleteTask(long docStartId, long docEndId){
        this.docStartId = docStartId;
        this.docEndId = docEndId;

        List<String> allIPs = new ArrayList<String>();
        allIPs.add("192.168.1.174");
        allIPs.add("192.168.1.231");
        allIPs.add("192.168.1.140");
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void multiDeleteAll(List<Long> docIdList) {
        try {
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            for (long docId : docIdList) {
                bulkRequest.add(client.prepareDelete(indexNameAll, type, docId+""));
            }
            BulkResponse bulkResponse = bulkRequest.get();
            //comment: 大概是实现了有错重传
            while(bulkResponse.hasFailures()){
                System.out.println( "docID:" + docIdList.get(0) + bulkResponse.toString() );
//                for (long docId : docIdList) {
//                    bulkRequest.add(client.prepareDelete(indexNameAll, type, docId+""));
//                }
                bulkResponse = bulkRequest.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void multiDeleteHash(List<Long> docIdList) {
        try {
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            for (long docId : docIdList) {
                bulkRequest.add(client.prepareDelete((docId % 2000)+"", type, docId+""));
            }
            BulkResponse bulkResponse = bulkRequest.get();
            //comment: 大概是实现了有错重传
            while(bulkResponse.hasFailures()){
                System.out.println( "docID:" + docIdList.get(0) + bulkResponse.toString() );
//                for (long docId : docIdList) {
//                    bulkRequest.add(client.prepareDelete((docId % 2000)+"", type, docId+""));
//                }
                bulkResponse = bulkRequest.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            for (long i = docStartId; i < docEndId; i++) {
                long hashId = i % 2000;
                if (IndexMap.containsKey(hashId)){
                    IndexMap.get(hashId).add(i);
                } else {
                    ArrayList<Long> tempList = new ArrayList<Long>();
                    tempList.add(i);
                    IndexMap.put(hashId, tempList);
                }

                if ((IndexMap.get(hashId).size() > 0) && (IndexMap.get(hashId).size() > Math.round(Math.random()*200))){
                    multiDeleteHash(IndexMap.get(hashId));
                    ArrayList<Long> tempList = new ArrayList<Long>();
                    IndexMap.put(hashId, tempList);
                }

                docIdList.add(i);
                if (docIdList.size() == 100) {
                    System.out.println("deleting DOCID:" + i);
                    multiDeleteAll(docIdList);
                    docIdList.clear();
                }
            }

            for (long key : IndexMap.keySet()){
                if (IndexMap.get(key).size() > 0){
                    multiDeleteHash(IndexMap.get(key));
                }
            }

            //把剩余的灌进去
            if (docIdList.size() > 0) {
                multiDeleteAll(docIdList);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }

    }
}
