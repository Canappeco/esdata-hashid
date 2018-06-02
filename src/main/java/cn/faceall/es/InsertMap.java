package cn.faceall.es;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * 添加hashid和大索引的mapping
 * 执行命令：
 * nohup java -classpath ./target/ESDATA-0.0.1-SNAPSHOT-jar-with-dependencies.jar cn.faceall.es.InsertMap >> ./output.log 2>&1 &
 */

public class InsertMap {

    public static void main(String[] args) {
        TransportClient client;
        String indexNameAll = "2010";
        String cluster_name = "my-es";
        int number_of_shards_all = 32;
        int number_of_replicas_all = 0;
        int number_of_shards_hashid = 5;
        int number_of_replicas_hashid = 0;
        int refresh_interval = 30;

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
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(allIPs.get(0)), 9300))
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(allIPs.get(1)), 9300));
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(allIPs.get(2)), 9300));
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(allIPs.get(0)), 9300));


//            //总的索引
//            if (!isExistsIndex(indexNameAll, client)) {
//                try {
//                    System.out.println("index not exist");
//                    client.admin().indices().prepareCreate(indexNameAll)
//                            .setSettings(Settings.builder()
//                                    .put("index.number_of_shards", number_of_shards_all)
//                                    .put("index.number_of_replicas", number_of_replicas_all))
////                                    .put("index.refresh_interval", refresh_interval))
//                            .get();
//                    System.out.println("success");
//                } catch (Exception e) {
//                    System.out.println("create face_all_es error!");
//                    e.printStackTrace();
//                }
//            }
//            addMappingAll(client, indexNameAll, "testData");
//
//            //每一个HashId的索引
//            for (int i = 0; i < 2000; i++){
//                if (!isExistsIndex(i+"", client)) {
//                    try {
//                        System.out.println("index not exist");
//                        client.admin().indices().prepareCreate(i+"")
//                                .setSettings(Settings.builder()
//                                        .put("index.number_of_shards", number_of_shards_hashid)
//                                        .put("index.number_of_replicas", number_of_replicas_hashid))
////                                        .put("index.refresh_interval", -1))
//                                .get();
//                        System.out.println("success");
//                    } catch (Exception e) {
//                        System.out.println("create index-hashid error!");
//                        e.printStackTrace();
//                    }
//                }
//                addMappingSingleHash(client, i+"", "testData");
//            }

            //单独建一个索引的mapping, index名称为2003
            if (!isExistsIndex(indexNameAll, client)) {
                try {
                    System.out.println("index not exist");
                    client.admin().indices().prepareCreate(indexNameAll)
                            .setSettings(Settings.builder()
                                    .put("index.number_of_shards", number_of_shards_hashid)
                                    .put("index.number_of_replicas", number_of_replicas_hashid))
//                                    .put("index.refresh_interval", -1))
                            .get();
                    System.out.println("success");
                } catch (Exception e) {
                    System.out.println("create index-hashid error!");
                    e.printStackTrace();
                }
            }
            addMappingAll2(client, indexNameAll, "testData");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean isExistsIndex(String index, TransportClient client){
        IndicesExistsResponse response =
                client.admin().indices().prepareExists(index).execute().actionGet();
        return response.isExists();
    }

    public static void addMappingAll(TransportClient client, String index, String type) {
        try{
            client.admin().indices().preparePutMapping(index).setType(type).setSource(jsonBuilder().startObject()
                            .startObject(type)
                            .startObject("properties")
//                            .startObject("id").field("type", "string").endObject()
                            .startObject("camera_id").field("type", "string").endObject()
                            .startObject("camera_name").field("type", "string").endObject()
                            .startObject("person_id").field("type", "integer").endObject()
                            .startObject("age").field("type", "integer").endObject()
                            .startObject("gender").field("type", "integer").endObject()
                            .startObject("color").field("type", "integer").endObject()
                            .startObject("alarm").field("type", "boolean").endObject()
                            .startObject("hashId").field("type", "integer").endObject()
                            .startObject("in_time").field("type", "date").field("format", "yyyy-MM-dd HH:mm:ss").endObject()
                            .startObject("off_time").field("type", "date").field("format", "yyyy-MM-dd HH:mm:ss").endObject()
                            .startObject("small_face_path").field("type", "text").field("index", "false").endObject()
                            .startObject("match").field("type", "string").endObject()
                            .endObject()
                            .endObject()
                            .endObject()
            ).execute().actionGet();
            System.out.println("finish adding mapping------" + index);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void addMappingAll2(TransportClient client, String index, String type) {
        try{
            client.admin().indices().preparePutMapping(index).setType(type).setSource(jsonBuilder().startObject()
                            .startObject(type)
                            .startObject("properties")
//                            .startObject("id").field("type", "string").endObject()
                            .startObject("device_number").field("type", "string").endObject()
                            .startObject("camera_name").field("type", "string").endObject()
                            .startObject("age").field("type", "integer").endObject()
                            .startObject("gender").field("type", "integer").endObject()
                            .startObject("color").field("type", "string").endObject()
                            .startObject("alarm").field("type", "string").endObject()
                            .startObject("in_time").field("type", "date").field("format", "yyyy-MM-dd HH:mm:ss").endObject()
                            .startObject("off_time").field("type", "date").field("format", "yyyy-MM-dd HH:mm:ss").endObject()
                            .startObject("small_face_path").field("type", "text").field("index", "false").endObject()
                            .startObject("feature").field("type", "float").field("index", "false").endObject()
//                            .startObject("quality_score").field("type", "float").endObject()
                            .endObject()
                            .endObject()
                            .endObject()
            ).execute().actionGet();
            System.out.println("finish adding mapping------" + index);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void addMappingHashId(TransportClient client, String index, String type) {
        try{
            client.admin().indices().preparePutMapping(index).setType(type).setSource(jsonBuilder().startObject()
                            .startObject(type)
                            .startObject("properties")
                            .startObject("faceFeature")
                                    .field("type", "float")
//                                    .field("index", "false")
                            .endObject()
//                            .startObject("small_face_path").field("type", "text").field("index", "false").endObject()
                            .endObject()
                            .endObject()
                            .endObject()
            ).execute().actionGet();
            System.out.println("finish adding mapping------" + index);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void addMappingSingleHash(TransportClient client, String index, String type) {
        try{
            client.admin().indices().preparePutMapping(index).setType(type).setSource(jsonBuilder().startObject()
                            .startObject(type)
                            .startObject("properties")
                            .startObject("item0").field("type", "float").endObject()
                            .startObject("item1").field("type", "float").endObject()
                            .startObject("item2").field("type", "float").endObject()
                            .startObject("item3").field("type", "float").endObject()
                            .startObject("item4").field("type", "float").endObject()
                            .startObject("item5").field("type", "float").endObject()
                            .startObject("item6").field("type", "float").endObject()
                            .startObject("item7").field("type", "float").endObject()
                            .startObject("item8").field("type", "float").endObject()
                            .startObject("item9").field("type", "float").endObject()
                            .startObject("item10").field("type", "float").endObject()
                            .startObject("item11").field("type", "float").endObject()
                            .startObject("item12").field("type", "float").endObject()
                            .startObject("item13").field("type", "float").endObject()
                            .startObject("item14").field("type", "float").endObject()
                            .startObject("item15").field("type", "float").endObject()
                            .startObject("item16").field("type", "float").endObject()
                            .startObject("item17").field("type", "float").endObject()
                            .startObject("item18").field("type", "float").endObject()
                            .startObject("item19").field("type", "float").endObject()
                            .startObject("item20").field("type", "float").endObject()
                            .startObject("item21").field("type", "float").endObject()
                            .startObject("item22").field("type", "float").endObject()
                            .startObject("item23").field("type", "float").endObject()
                            .startObject("item24").field("type", "float").endObject()
                            .startObject("item25").field("type", "float").endObject()
                            .startObject("item26").field("type", "float").endObject()
                            .startObject("item27").field("type", "float").endObject()
                            .startObject("item28").field("type", "float").endObject()
                            .startObject("item29").field("type", "float").endObject()
                            .startObject("item30").field("type", "float").endObject()
                            .startObject("item31").field("type", "float").endObject()
                            .startObject("item32").field("type", "float").endObject()
                            .startObject("item33").field("type", "float").endObject()
                            .startObject("item34").field("type", "float").endObject()
                            .startObject("item35").field("type", "float").endObject()
                            .startObject("item36").field("type", "float").endObject()
                            .startObject("item37").field("type", "float").endObject()
                            .startObject("item38").field("type", "float").endObject()
                            .startObject("item39").field("type", "float").endObject()
                            .startObject("item40").field("type", "float").endObject()
                            .startObject("item41").field("type", "float").endObject()
                            .startObject("item42").field("type", "float").endObject()
                            .startObject("item43").field("type", "float").endObject()
                            .startObject("item44").field("type", "float").endObject()
                            .startObject("item45").field("type", "float").endObject()
                            .startObject("item46").field("type", "float").endObject()
                            .startObject("item47").field("type", "float").endObject()
                            .startObject("item48").field("type", "float").endObject()
                            .startObject("item49").field("type", "float").endObject()
                            .startObject("item50").field("type", "float").endObject()
                            .startObject("item51").field("type", "float").endObject()
                            .startObject("item52").field("type", "float").endObject()
                            .startObject("item53").field("type", "float").endObject()
                            .startObject("item54").field("type", "float").endObject()
                            .startObject("item55").field("type", "float").endObject()
                            .startObject("item56").field("type", "float").endObject()
                            .startObject("item57").field("type", "float").endObject()
                            .startObject("item58").field("type", "float").endObject()
                            .startObject("item59").field("type", "float").endObject()
                            .startObject("item60").field("type", "float").endObject()
                            .startObject("item61").field("type", "float").endObject()
                            .startObject("item62").field("type", "float").endObject()
                            .startObject("item63").field("type", "float").endObject()
                            .startObject("item64").field("type", "float").endObject()
                            .startObject("item65").field("type", "float").endObject()
                            .startObject("item66").field("type", "float").endObject()
                            .startObject("item67").field("type", "float").endObject()
                            .startObject("item68").field("type", "float").endObject()
                            .startObject("item69").field("type", "float").endObject()
                            .startObject("item70").field("type", "float").endObject()
                            .startObject("item71").field("type", "float").endObject()
                            .startObject("item72").field("type", "float").endObject()
                            .startObject("item73").field("type", "float").endObject()
                            .startObject("item74").field("type", "float").endObject()
                            .startObject("item75").field("type", "float").endObject()
                            .startObject("item76").field("type", "float").endObject()
                            .startObject("item77").field("type", "float").endObject()
                            .startObject("item78").field("type", "float").endObject()
                            .startObject("item79").field("type", "float").endObject()
                            .startObject("item80").field("type", "float").endObject()
                            .startObject("item81").field("type", "float").endObject()
                            .startObject("item82").field("type", "float").endObject()
                            .startObject("item83").field("type", "float").endObject()
                            .startObject("item84").field("type", "float").endObject()
                            .startObject("item85").field("type", "float").endObject()
                            .startObject("item86").field("type", "float").endObject()
                            .startObject("item87").field("type", "float").endObject()
                            .startObject("item88").field("type", "float").endObject()
                            .startObject("item89").field("type", "float").endObject()
                            .startObject("item90").field("type", "float").endObject()
                            .startObject("item91").field("type", "float").endObject()
                            .startObject("item92").field("type", "float").endObject()
                            .startObject("item93").field("type", "float").endObject()
                            .startObject("item94").field("type", "float").endObject()
                            .startObject("item95").field("type", "float").endObject()
                            .startObject("item96").field("type", "float").endObject()
                            .startObject("item97").field("type", "float").endObject()
                            .startObject("item98").field("type", "float").endObject()
                            .startObject("item99").field("type", "float").endObject()
                            .startObject("item100").field("type", "float").endObject()
                            .startObject("item101").field("type", "float").endObject()
                            .startObject("item102").field("type", "float").endObject()
                            .startObject("item103").field("type", "float").endObject()
                            .startObject("item104").field("type", "float").endObject()
                            .startObject("item105").field("type", "float").endObject()
                            .startObject("item106").field("type", "float").endObject()
                            .startObject("item107").field("type", "float").endObject()
                            .startObject("item108").field("type", "float").endObject()
                            .startObject("item109").field("type", "float").endObject()
                            .startObject("item110").field("type", "float").endObject()
                            .startObject("item111").field("type", "float").endObject()
                            .startObject("item112").field("type", "float").endObject()
                            .startObject("item113").field("type", "float").endObject()
                            .startObject("item114").field("type", "float").endObject()
                            .startObject("item115").field("type", "float").endObject()
                            .startObject("item116").field("type", "float").endObject()
                            .startObject("item117").field("type", "float").endObject()
                            .startObject("item118").field("type", "float").endObject()
                            .startObject("item119").field("type", "float").endObject()
                            .startObject("item120").field("type", "float").endObject()
                            .startObject("item121").field("type", "float").endObject()
                            .startObject("item122").field("type", "float").endObject()
                            .startObject("item123").field("type", "float").endObject()
                            .startObject("item124").field("type", "float").endObject()
                            .startObject("item125").field("type", "float").endObject()
                            .startObject("item126").field("type", "float").endObject()
                            .startObject("item127").field("type", "float").endObject()
                            .endObject()
                            .endObject()
                            .endObject()
            ).execute().actionGet();
            System.out.println("finish adding mapping------" + index);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
