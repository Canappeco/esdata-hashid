package cn.faceall.es;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.net.InetAddress;
import java.util.*;
import java.text.SimpleDateFormat;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;

public class InsertTask implements Runnable{

    long docStartId;
    long docEndId;
    List<String> allTypes;
    List<String> allColors;
    TransportClient client;
    String cluster_name = "my-es";
    String indexNameAll = "2010";
    String type = "testData";

    public InsertTask(long start, long end) {

        this.docStartId = start;
        this.docEndId = end;
        this.allTypes = new ArrayList<String>();
        allTypes.add("2017-06");
        allTypes.add("2017-07");
        allTypes.add("2017-08");
        allTypes.add("2017-09");
        this.allColors = new ArrayList<String>();
        allColors.add("black");
        allColors.add("white");
        allColors.add("yellow");
        allColors.add("gray");
        allColors.add("red");
        allColors.add("orange");
        allColors.add("green");
        allColors.add("blue");
        allColors.add("purple");

        List<String> allIPs = new ArrayList<String>();
//        allIPs.add("192.168.1.140");
//        allIPs.add("192.168.1.174");
//        allIPs.add("192.168.1.231");
        allIPs.add("127.0.0.1");
        Collections.shuffle(allIPs);

        Settings settings = Settings.builder()
                .put("cluster.name", cluster_name)
                .put("client.transport.sniff", true)
                .build();

        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(allIPs.get(0)), 9300));
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(allIPs.get(1)), 9300));
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(allIPs.get(2)), 9300));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean isExistsIndex(String index, TransportClient client){
        IndicesExistsResponse  response =
                client.admin().indices().prepareExists(index).execute().actionGet();
        return response.isExists();
    }

    public static class DOCDATA{
        public String docId;
        int hash_id;
        double face_feature[] = new double[128];
        String device_number;
        String camera_name;
        int age;
        int gender;
        int color;
        boolean alarm;
        Date in_time;
        Date off_time;
        String small_face_path;
        boolean has_feature;
        String match;
    }

    List<DOCDATA> docsList = new ArrayList<DOCDATA>();

    public void multiPutAll(List<DOCDATA> docsList) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            for (DOCDATA cc : docsList) {
                bulkRequest.add(client.prepareIndex(indexNameAll, type, cc.docId).setSource(
                        jsonBuilder().startObject()
//                                .field("hash_id", cc.hash_id)
                                .field("feature", cc.face_feature)
//                                .field("in_time", df.format(cc.in_time))
//                                .field("off_time", df.format(cc.off_time))
                                .field("in_time", df.format(new Date()))
                                .field("off_time", df.format(new Date()))
                                .field("device_number", cc.device_number)
                                .field("camera_name", cc.camera_name)
                                .field("age", cc.age)
                                .field("gender", cc.gender)
                                .field("color", allColors.get(cc.color))
                                .field("alarm", cc.alarm)
                                .field("has_feature", cc.has_feature)
                                .field("small_face_path", cc.small_face_path)
//                                .field("match", cc.match)
                                .endObject()));
            }
            BulkResponse bulkResponse = bulkRequest.get();
            //comment: 大概是实现了有错重传，重传时不用重新添加内容
            while(bulkResponse.hasFailures()){
                System.out.println( "docID:" + docsList.get(0).docId + bulkResponse.toString() );
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
                currdata.docId = ""+simpleDateFormat.format(new Date())+"_"+Math.random();
                currdata.hash_id = (int)(i % 2000);
                for (int j = 0; j < 128; j++) {
                    currdata.face_feature[j] = Math.random()*2-1+j*2;
//                    currdata.face_feature[j] = Math.random()*2-1;

                }
                currdata.device_number = (i % 100)+"";
                currdata.camera_name = "name-"+(i % 100)+"";
                currdata.age = (int)(i % 100);
                currdata.gender = (int)(i % 2);
                currdata.color = (int)(i % 8);
                currdata.alarm = false;
                currdata.has_feature = true;
                currdata.match = "match";
                currdata.small_face_path = "/home/es/face_img/" + i + ".jpg";
                Calendar tmp = Calendar.getInstance();
                tmp.setTime(new Date());
                tmp.set(Calendar.YEAR, 2017);
                tmp.set(Calendar.MONTH, (int) ((i % 4) + 5));
                tmp.set(Calendar.DAY_OF_MONTH, (int) (i % 31));
                tmp.set(Calendar.HOUR_OF_DAY, (int) (i % 24));
                tmp.set(Calendar.MINUTE, (int) (i % 60));
                tmp.set(Calendar.SECOND, (int) (i % 60));
                currdata.in_time = tmp.getTime();
                currdata.off_time = tmp.getTime();

                docsList.add(currdata);
                //批量插入
                if (docsList.size() == 100) {
                    System.out.println("inserting DOCID:" + i);
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

    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
