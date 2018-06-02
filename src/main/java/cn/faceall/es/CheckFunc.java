package cn.faceall.es;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class CheckFunc {

    public static void main(String[] args) {
        TransportClient client;
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

        String jsonString = "{\"id\":\"6_2017-10-10_18:13:36_57513\",\"camera_id\":\"6\",\"camera_name\":\"name_6\",\"person_id\":-1,\"small_face_path\":\"2017-10-10/18/6/6_2017-10-10_18:13:36_57510.jpg\",\"age\":3,\"gender\":0,\"color\":6,\"alarm\":false,\"in_time\":\"2017-10-10 18:13:36\",\"hashId\":49073,\"match\":\"{\\\"result\\\":[],\\\"num\\\":0}\",\"faceFeature\":[0.12429692596197128,-0.15823473036289216,-0.06020466238260269,0.08470992743968964,-0.026675080880522729,-0.06579184532165528,-0.10000605136156082,-0.02451368048787117,0.12512415647506715,0.11887193471193314,-0.08201039582490921,-0.010247952304780484,0.21061033010482789,0.12099556624889374,-0.07431507110595703,0.04158160835504532,0.10103317350149155,-0.012561650015413762,0.010558251291513443,0.2048361599445343,-0.04076889902353287,-0.07762926816940308,0.026723118498921396,-0.06588578969240189,0.05087307095527649,0.12706127762794496,0.07071150839328766,-0.0061872233636677269,0.06662248075008393,0.00796512421220541,-0.011255658231675625,0.055205125361680987,-0.03802158683538437,-0.014504632912576199,0.06741204857826233,0.018665863201022149,0.1123456209897995,-0.051908161491155627,0.09198710322380066,-0.10792156308889389,-0.013384842313826085,0.03819157928228378,0.016475746408104898,0.1267164945602417,-0.0560908205807209,0.10326948761940003,-0.09740832448005676,0.11811202019453049,0.059200067073106769,0.06525549292564392,-0.12832681834697724,-0.009107532911002636,0.06740506738424301,-0.14103855192661286,-0.020281760022044183,0.20834758877754212,0.05796421691775322,-0.13887044787406922,-0.10713979601860047,-0.039743777364492419,0.027247607707977296,-0.09635838866233826,0.03816337138414383,0.09645051509141922,0.013582181185483933,0.08920586854219437,0.03875989094376564,-0.10696008801460266,-0.08790500462055206,-0.01523356419056654,-0.2438180148601532,0.10001549124717713,0.02590854838490486,0.026262976229190828,0.035301756113767627,0.054254114627838138,0.16365614533424378,0.04487517103552818,-0.05808086693286896,-0.17998114228248597,0.06342804431915283,0.1235312893986702,0.06581394374370575,-0.012759569101035595,-0.1236136257648468,-0.059715792536735538,-0.06173115596175194,0.052050381898880008,0.002741046017035842,-0.027531933039426805,-0.09128545224666596,-0.11221685260534287,0.12273228913545609,0.03062501549720764,-0.019707176834344865,-0.030157238245010377,0.0407690554857254,-0.028209352865815164,0.1306181401014328,-0.09154310822486878,0.024846887215971948,-0.08862130343914032,0.09481752663850784,0.06021759659051895,0.10639811307191849,0.02695874683558941,-0.002501961076632142,-0.06418982148170471,0.0164998397231102,0.06780190765857697,0.03199143707752228,0.04318973422050476,-0.023439986631274225,-0.060659587383270267,-0.07124283909797669,-0.09367770701646805,-0.12549126148223878,0.045252665877342227,0.09461617469787598,-0.20681104063987733,0.20045727491378785,-0.02060348354279995,0.09775136411190033,0.11527518182992935,0.05898113176226616,0.04884573817253113,-0.020447859540581704,0.018303243443369867],\"off_time\":\"2017-10-10 18:13:37\"}";
        JSONObject jsonObject = new JSONObject(jsonString);

        try{
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(allIPs.get(0)), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(allIPs.get(1)), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(allIPs.get(2)), 9300));
            IndexResponse indexResponse = client.prepareIndex("test_all", "testData", jsonObject.getString("id")).setSource(jsonBuilder()
                    .startObject()
                    .field("camera_id", jsonObject.get("camera_id"))
                    .field("camera_name", jsonObject.get("camera_name"))
                    .field("person_id", jsonObject.get("person_id"))
                    .field("age",jsonObject.get("age"))
                    .field("gender", jsonObject.get("gender"))
                    .field("color", jsonObject.get("color"))
                    .field("alarm", jsonObject.get("alarm"))
                    .field("hashId", jsonObject.get("hashId"))
                    .field("in_time", jsonObject.get("in_time"))
                    .field("off_time", jsonObject.get("off_time"))
                    .field("small_face_path", jsonObject.get("small_face_path"))
                    .field("match", jsonObject.get("match"))
                    .endObject())
                    .get();
            System.out.println("test_all "+jsonObject.get("id")+" "+indexResponse.status());
            if (jsonObject.get("hashId") != "" && !jsonObject.get("hashId").equals("")){
                int index = jsonObject.getInt("hashId") % 2000;
                System.out.println(index);
                IndexResponse indexResponse1 = client.prepareIndex( index+"", "testData", jsonObject.getString("id")).setSource(jsonBuilder()
                        .startObject()
                        .field("faceFeature", jsonObject.get("faceFeature"))
                        .endObject())
                        .get();
                System.out.println("hashid "+jsonObject.get("id")+" "+indexResponse1.status());

            }
            System.out.println(jsonObject.get("id") + " succeed to add to ElasticSearch!");
        } catch (Exception e) {
            System.out.println(jsonObject.get("id") + " fail to add to ElasticSearch!");
            e.printStackTrace();
        }
    }

}
