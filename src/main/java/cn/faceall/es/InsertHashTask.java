package cn.faceall.es;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
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

public class InsertHashTask implements Runnable{

    long docStartId;
    long docEndId;
    List<String> allTypes;
    List<String> allColors;
    TransportClient client;
    String cluster_name = "face_all_es";
    String indexName = "";
    String type = "testData";
    long hashid = 0;

    public InsertHashTask(long docStartId, long docEndId, long hashid) {

        this.docStartId = docStartId;
        this.docEndId = docEndId;
        this.indexName = hashid+"";
        this.hashid = hashid;
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

    public boolean isExistsIndex(String index, TransportClient client){
        IndicesExistsResponse response =
                client.admin().indices().prepareExists(index).execute().actionGet();
        return response.isExists();
    }

    public static class DOCDATA{
        public String docId;
        long hashId;
        double faceFeature[] = new double[128];
        String small_face_path;
    }

    List<DOCDATA> docsList = new ArrayList<DOCDATA>();

    public void multiPutAll(List<DOCDATA> docsList) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            for (DOCDATA cc : docsList) {
                bulkRequest.add(client.prepareIndex(indexName, type, cc.docId).setSource(
                        jsonBuilder().startObject()
                                .field("faceFeature", cc.faceFeature)
                                .field("small_face_path", cc.small_face_path)
                                .endObject()));
            }
            BulkResponse bulkResponse = bulkRequest.get();
            //comment: 大概是实现了有错重传
            while(bulkResponse.hasFailures()){
                //BulkRequestBuilder bulkRequest = client.prepareBulk();
                System.out.println( "docID:" + docsList.get(0).docId + bulkResponse.toString() );
//                for (DOCDATA cc : docsList) {
//                    bulkRequest.add(client.prepareIndex(indexName, type, cc.docId).setSource(
//                            jsonBuilder().startObject()
//                                    .field("faceFeature", cc.faceFeature)
//                                    .field("small_face_path", cc.small_face_path)
//                                    .endObject()));
//                }
                bulkResponse = bulkRequest.get();
            }
			/*if (bulkResponse.hasFailures()) {
				System.out.println( "docID:" + docsList.get(0).docId + bulkResponse.toString() );
			}*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void run() {
        try {
//            DecimalFormat decimalFormat = new DecimalFormat( "0.000000");
            for (long i = docStartId; i < docEndId; i++) {
                DOCDATA currdata = new DOCDATA();
                currdata.docId = hashid + 2000*i + "";
                currdata.hashId = hashid;
                for (int j = 0; j < 128; j++) {
                    currdata.faceFeature[j] = Math.random()*2-1+j*2;
                }
                currdata.small_face_path = "/home/es/face_img/" + currdata.docId + ".jpg";

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
