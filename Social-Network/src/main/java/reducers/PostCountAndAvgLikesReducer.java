package reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PostCountAndAvgLikesReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        Double averageLikesPerPostCategory = 0.0;
        Integer postCount = 0;
        Integer numberOfLikes = 0;
        for (Text t : values) {
            String[] categoryData = t.toString().split(",");
            if (categoryData.length == 2) {
                postCount += Integer.valueOf(categoryData[0]);
                numberOfLikes += Integer.valueOf(categoryData[1]);
            }
        }
        averageLikesPerPostCategory = ((double) numberOfLikes / (double) postCount);
        //round into two decimal places
        averageLikesPerPostCategory = Math.round(averageLikesPerPostCategory * 100.0) / 100.0;

        context.write(new Text(key.toString()+","+postCount + "," + numberOfLikes + ","
                + averageLikesPerPostCategory), new Text(" "));
    }
}
