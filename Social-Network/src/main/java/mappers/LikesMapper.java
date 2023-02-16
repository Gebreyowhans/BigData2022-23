package mappers;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utilities.LikesInputDataModel;
import java.io.IOException;

public class LikesMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        if (data.length == LikesInputDataModel.COLUMN_COUNT) {
            context.write(new Text(data[1]), new Text("Likes," +
                    data[0]));
        }
    }
}
