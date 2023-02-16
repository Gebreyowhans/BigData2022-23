package mappers;

import models.PostsHBaseModel;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class PostCountAndAvgLikesMapper extends TableMapper<Text, Text> {
    private Text outputKey = new Text();
    @Override
    protected void map(ImmutableBytesWritable key, Result value,
                       Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
            throws IOException, InterruptedException {
        // Extract user ID, category, and number of likes from the HBase row
        String userEmail = Bytes.toString(value.getValue(PostsHBaseModel.
                POST_INFO_FAMILY, PostsHBaseModel.POST_USER_EMAIL_QUALIFIER));
        String category = Bytes.toString(value.getValue(PostsHBaseModel.
                POST_INFO_FAMILY, PostsHBaseModel.POST_CATEGORY_QUALIFIER));
        int likes = Bytes.toInt(value.getValue(PostsHBaseModel.POST_LIKES_FAMILY,
                PostsHBaseModel.NUMBER_OF_LIKES_QUALIFIER));
        // Construct the composite key using the user ID and category
        outputKey.set(userEmail + "," + category);
        // Emit the composite key and the number of likes as the value
        context.write(outputKey, new Text(String.valueOf(likes)));
    }
}
