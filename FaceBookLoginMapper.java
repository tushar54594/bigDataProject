import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FacebookLoginMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");

        // Assuming the input format is timestamp, user_id, region
        if (tokens.length == 3) {
            String timestamp = tokens[0];
            String userId = tokens[1];
            String region = tokens[2];

            // Output user_id as the key and a tuple (timestamp, region) as the value
            context.write(new Text(userId), new Text(timestamp + "," + region));
        }
    }
}
