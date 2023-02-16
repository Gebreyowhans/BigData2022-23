package combiners;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utilities.Constants;

import java.io.IOException;

public class PostCountAndAvgLikesCombiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        Integer categoryPostCount = 0;
        Integer sumOfLikesPerCategory = 0;
        for (Text t : values) {
            String[] categoryData = t.toString().split(",");
            if (categoryData.length == 1) {
                categoryPostCount += 1;
                sumOfLikesPerCategory += Integer.valueOf(categoryData[0]);
            }
        }
        context.write(key, new Text(categoryPostCount + "," + sumOfLikesPerCategory));
    }
}
