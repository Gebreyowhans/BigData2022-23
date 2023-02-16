package reducers;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import models.PostsHBaseModel;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PostLikesReducer extends TableReducer<Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Reducer<Text, Text, NullWritable, Mutation>.Context context)
            throws IOException, InterruptedException {
        String category = "";
        String date = "";
        String userEmail = "";
        JsonArray usersLikedPostId = new JsonArray();
        for (Text t : values) {
            String data[] = t.toString().split(",");
            if (data[0].equals("Likes")) {
                JsonObject user = new JsonObject();
                user.addProperty("user", data[1]);
                usersLikedPostId.add(user);
            } else if (data[0].equals("Post")) {
                category = data[1];
                date = data[2];
                userEmail = data[3];// posts the email
            }
        }

        //row key (postid_date)
        Put p = new Put(Bytes.toBytes(key + "_" + date));
        //info column family
        p.addColumn(PostsHBaseModel.POST_INFO_FAMILY,
                PostsHBaseModel.POST_CATEGORY_QUALIFIER,
                Bytes.toBytes(category));
        p.addColumn(PostsHBaseModel.POST_INFO_FAMILY,
                PostsHBaseModel.POST_USER_EMAIL_QUALIFIER,
                Bytes.toBytes(userEmail));
        //likes column family
        p.addColumn(PostsHBaseModel.POST_LIKES_FAMILY,
                PostsHBaseModel.USERS_QUALIFIER,
                Bytes.toBytes(String.valueOf(usersLikedPostId)));
        // which holds the count of users who liked a particular post
        p.addColumn(PostsHBaseModel.POST_LIKES_FAMILY,
                PostsHBaseModel.NUMBER_OF_LIKES_QUALIFIER,
                Bytes.toBytes(Integer.valueOf(usersLikedPostId.size())));
        //write into an HBase table
        context.write(null, p);
    }
}
