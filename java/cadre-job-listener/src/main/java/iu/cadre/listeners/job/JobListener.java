package iu.cadre.listeners.job;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.ArrayList;
import java.util.List;

public class JobListener {
    private static final String QUEUE_NAME = "testQueue";
    private static final JsonParser jsonParser = new JsonParser();
    public static void main(String[] args) {

    }

    public static void poll_queue(){
        AwsSessionCredentials awsCreds = AwsSessionCredentials.create(
                "your_access_key_id_here",
                "your_secret_key_id_here",
                "your_session_token_here");
        SqsClient sqsClient = SqsClient.builder().credentialsProvider(StaticCredentialsProvider.create(awsCreds)).build();
        List<String> outputFiltersSingle = new ArrayList<String>();

        while (true){
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(QUEUE_NAME)
                    .build();

            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

//            	'Body': '{"job_name": "Test", "filters": [{"field": "year", "value": "2005", "operation": "AND"}, {"field": "journal_display_name", "value": "science", "operation": ""}], "output": [{"field": "paper_id", "type": "single"}, {"field": "year", "type": "single"}, {"field": "original_title", "type": "single"}, {"field": "authors_display_name", "type": "single"}, {"field": "journal_display_name", "type": "single"}, {"field": "citations", "type": "network", "degree": 1}], "dataset": "mag", "job_id": "5d3f63cf-3f49-4a24-9b0f-3461a0485078", "username": "mjzwk4tsmv2hi", "user_id": 25}'
            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
            // print out the messages
            for (Message m : messages) {
                String messageBody = m.body();
                JsonElement messageBodyJElement = jsonParser.parse(messageBody);
                JsonObject messageBodyJObj = messageBodyJElement.getAsJsonObject();
                String dataset = messageBodyJObj.get("dataset").getAsString();
                String jobId = messageBodyJObj.get("job_id").getAsString();
                String jobName = messageBodyJObj.get("job_name").getAsString();
                String userName = messageBodyJObj.get("username").getAsString();
                String userId = messageBodyJObj.get("user_id").getAsString();
                JsonArray filters = messageBodyJObj.get("filters").getAsJsonArray();
                JsonArray outputFields = messageBodyJObj.get("output").getAsJsonArray();

                for(int i=0; i < outputFields.size(); i++){
                    JsonObject outputField = outputFields.get(i).getAsJsonObject();
                    String type = outputField.get("type").getAsString();
                    if (type.equals("single")){
                        String field = outputField.get("field").getAsString();
                        if(field.equals("wos_id")){
                            outputFiltersSingle.add("id");
                        }else if (field.equals("references")){
//                            TODO : escape ' character
                            outputFiltersSingle.add("references");
                        }else {
                            outputFiltersSingle.add(field);
                        }
                    }else {
                       String networkQueryType = outputField.get("field").getAsString();
                       int degree = outputField.get("degree").getAsInt();
                    }
                }

            }
        }
    }
}
