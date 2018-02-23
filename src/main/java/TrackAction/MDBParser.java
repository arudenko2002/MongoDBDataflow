package TrackAction;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONArray;
import org.json.JSONObject;

public class MDBParser extends DoFn<org.bson.Document,TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        org.bson.Document line = c.element();
        JSONObject obj = new JSONObject(line);
        if(obj.keySet().contains("firstName")
                && obj.keySet().contains("lastName")
                && obj.keySet().contains("email")
                && obj.keySet().contains("interests")
                && obj.keySet().contains("active") && Boolean.parseBoolean(obj.get("active").toString())
                && obj.keySet().contains("subscriptions") && Boolean.parseBoolean(obj.getJSONObject("subscriptions").get("playlistSubscription").toString())
                ) {
            String firstName = obj.getString("firstName");
            String lastName = obj.getString("lastName");
            String email = obj.getString("email");
            JSONObject o = (JSONObject) obj.get("interests");
            JSONArray conopusids = (JSONArray) o.get("artists");
            for (int j = 0; j < conopusids.length(); j++) {
                TableRow tr = new TableRow();
                tr.set("firstName", firstName);
                tr.set("lastName", lastName);
                tr.set("email", email);
                String conopus_id = conopusids.get(j).toString();
                tr.set("conopus_id", conopus_id);
                c.output(tr);
                System.out.println(firstName+","+lastName+","+email+","+conopus_id);
            }
        }
    }
}
