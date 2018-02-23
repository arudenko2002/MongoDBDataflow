package TrackAction;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableRow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.ArrayList;
import java.util.List;

public class MongoDBDataFlow {
    static String user  = "swift-dev-admin";
    static String password = "eef9XeifIemiech8";

    static String host = "35.197.2.144";
    static int port = 27017;
    static String databaseA = "sst-dizzee-dev";
    static String collectionA = "users";

    static String uri = "mongodb://"+user+":"+password+"@"+host+":"+port+"/?authMechanism=SCRAM-SHA-1&authSource=admin";

    public void mongoDBDataFlow(Pipeline pipeline) {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("firstname").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lastname").setType("STRING"));
        fields.add(new TableFieldSchema().setName("email").setType("STRING"));
        fields.add(new TableFieldSchema().setName("conopus_id").setType("INTEGER"));
        TableSchema schema = new TableSchema().setFields(fields);

        System.out.println(uri);
        MongoDbIO.Read readMongoDB = MongoDbIO.read()
                .withUri(uri)
                .withDatabase(databaseA)
                .withCollection(collectionA);

        BigQueryIO.Write<TableRow> writeBigQuery = BigQueryIO.writeTableRows()
                .to("umg-tools:swift_trends_alerts.temp_output_table2")
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);

        pipeline.apply(readMongoDB)
                .apply(ParDo.of(new MDBParser()))
                .apply(writeBigQuery);
    }

    public void noveMongoDBtoBigQuery(String[] args) {
        System.out.println("MongoDBDataFlow started");
        org.apache.beam.sdk.options.PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://umg-dev/temp/dataflow");
        // Create the Pipeline object with the options we defined above.
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);
        mongoDBDataFlow(pipeline);
        pipeline.run().waitUntilFinish();
        System.out.println("End of MongoDB process");
    }

    public static void main(String args[]) {
        MongoDBDataFlow mdb = new MongoDBDataFlow();
        mdb.noveMongoDBtoBigQuery(args);
    }
}



