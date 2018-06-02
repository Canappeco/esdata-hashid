package cn.faceall.es;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class InsertSingleHashTask implements Runnable {

    long docStartId;
    long docEndId;
    TransportClient client;
    String cluster_name = "face_all_es";
    String indexName = "2003";
    String type = "testData";

    public InsertSingleHashTask(long docStartId, long docEndId) {

        this.docStartId = docStartId;
        this.docEndId = docEndId;

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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class DOCDATA{

        /**
         * .startObject("camera_id").field("type", "string").endObject()
         .startObject("camera_name").field("type", "string").endObject()
         .startObject("age").field("type", "integer").endObject()
         .startObject("gender").field("type", "integer").endObject()
         .startObject("color").field("type", "integer").endObject()
         .startObject("alarm").field("type", "boolean").endObject()
         .startObject("hash_id").field("type", "integer").endObject()
         .startObject("in_time").field("type", "date").field("format", "yyyy-MM-dd HH:mm:ss").endObject()
         .startObject("off_time").field("type", "date").field("format", "yyyy-MM-dd HH:mm:ss").endObject()
         .startObject("small_face_path").field("type", "text").field("index", "false").endObject()
         .startObject("face_feature").field("type", "float").field("index", "false").endObject()
         .startObject("has_feature").field("type", "boolean").endObject()
         .startObject("match").field("type", "string").endObject()
         */
        public String docId;
        public String camera_id;
        public String camera_name;
        public int age;
        public int gender;
        public int color;
        public boolean alarm;
        public int hash_id;
        public Date date;
        double faceFeature[] = new double[128];
    }

    List<DOCDATA> docsList = new ArrayList<DOCDATA>();

    public void multiPutAll(List<DOCDATA> docsList) {
        try {
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            for (DOCDATA cc : docsList) {
                XContentBuilder xContentBuilder = jsonBuilder().startObject();
                for (int i = 0; i < 128; i++) {
                    xContentBuilder = xContentBuilder.field("item"+i, cc.faceFeature[i]);
                }
                xContentBuilder = xContentBuilder.endObject();
                bulkRequest.add(client.prepareIndex(indexName, type, cc.docId).setSource(xContentBuilder));
            }
            BulkResponse bulkResponse = bulkRequest.get();
            //comment: 大概是实现了有错重传
            while(bulkResponse.hasFailures()){
                System.out.println("ERROR ----- " + bulkResponse.buildFailureMessage() );
                bulkResponse = bulkRequest.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void run() {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
            for (long i = docStartId; i < docEndId; i++) {
                DOCDATA currdata = new DOCDATA();
                Date date = new Date();
                currdata.docId = ""+simpleDateFormat.format(date)+"_"+Math.random();
                for (int j = 0; j < 128; j++) {
                    currdata.faceFeature[j] = Math.random()*2-1;
                }

                docsList.add(currdata);
                //批量插入
                if (docsList.size() == 100) {
                    System.out.println("inserting DOCID:" + currdata.docId);
                    multiPutAll(docsList);
                    docsList.clear();
                }
            }

            //把剩余的灌进去
            if (docsList.size() > 0) {
                multiPutAll(docsList);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }

    }
}
