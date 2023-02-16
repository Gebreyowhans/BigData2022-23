package jobs;

import HbaseTables.CreatePostsTable;
import combiners.PostCountAndAvgLikesCombiner;
import mappers.LikesMapper;
import mappers.PostCountAndAvgLikesMapper;
import mappers.PostsMapper;
import models.PostsHBaseModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducers.PostCountAndAvgLikesReducer;
import reducers.PostLikesReducer;
import utilities.Constants;

public class PostCountAndAvgLikesJob implements GenericJobBuilder {
    @Override
    public Job buildJob() {
        try {

            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", Constants.CLUSTER_MACHINES);
            Job job = Job.getInstance(conf, "Post count and average likes");

            job.setJarByClass(PostCountAndAvgLikesJob.class);

            Scan scan = new Scan();
            scan.addFamily(PostsHBaseModel.POST_INFO_FAMILY);
            scan.addFamily(PostsHBaseModel.POST_LIKES_FAMILY);
            scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
            scan.setCacheBlocks(false);  // don't set to true for MR jobs

            TableMapReduceUtil.initTableMapperJob(
                    PostsHBaseModel.TABLE_POSTS,// input HBase table name
                    scan,             // Scan instance to control CF and attribute selection
                    PostCountAndAvgLikesMapper.class,   // mapper
                    Text.class,             // mapper output key
                    Text.class,             // mapper output value
                    job);
            job.setCombinerClass(PostCountAndAvgLikesCombiner.class);//combiner class
            job.setReducerClass(PostCountAndAvgLikesReducer.class);  // reducer class

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            Path outputPath = new Path(Constants.OUTPUT_FILE_PATH);
            FileOutputFormat.setOutputPath(job, outputPath);
            outputPath.getFileSystem(conf).delete(outputPath);

            return job;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
