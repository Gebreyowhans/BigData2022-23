package mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utilities.LikesInputDataModel;
import utilities.PostInputDataModel;

import java.io.IOException;

public class PostsMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        if (data.length == PostInputDataModel.COLUMN_COUNT) {
            context.write(new Text(data[0]), new Text("Post," +
                    data[1] + "," + data[2] + "," + data[3]));
        }
    }
}
