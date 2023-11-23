import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FacebookLoginReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Map<String, Integer> loginCounts = new HashMap<>();
        Map<String, Integer> regionCounts = new HashMap<>();

        // Input format: user_id, timestamp,region
        for (Text value : values) {
            String[] loginInfo = value.toString().split(",");
            String timestamp = loginInfo[0];
            String region = loginInfo[1];

            // Extract the hour from the timestamp
            String hour = timestamp.split(" ")[1].split(":")[0];

            // Increment the login count for the hour
            loginCounts.put(hour, loginCounts.getOrDefault(hour, 0) + 1);

            // Increment the region count
            regionCounts.put(region, regionCounts.getOrDefault(region, 0) + 1);
        }

        // Find the peak usage time
        String peakHour = findMaxKey(loginCounts);

        // Find the most used region
        String mostUsedRegion = findMaxKey(regionCounts);

        // Output the results
        context.write(new Text("Peak Usage Time:"), new Text(peakHour));
        context.write(new Text("Most Used Region:"), new Text(mostUsedRegion));
    }

    private String findMaxKey(Map<String, Integer> map) {
        String maxKey = null;
        int maxValue = Integer.MIN_VALUE;

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entry.getValue() > maxValue) {
                maxKey = entry.getKey();
                maxValue = entry.getValue();
            }
        }

        return maxKey;
    }
}
