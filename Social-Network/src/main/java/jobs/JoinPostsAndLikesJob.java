package jobs;

import HbaseTables.CreatePostsTable;
import mappers.LikesMapper;
import mappers.PostsMapper;
import models.PostsHBaseModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducers.PostLikesReducer;
import utilities.Constants;

public class JoinPostsAndLikesJob implements GenericJobBuilder {
    @Override
    public Job buildJob() {
        try {

            //create Hbase table posts
            CreatePostsTable.createTable();

            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", Constants.CLUSTER_MACHINES);
            Job job = Job.getInstance(conf, "Join Posts and Likes");

            job.setJarByClass(JoinPostsAndLikesJob.class);

            MultipleInputs.addInputPath(job, new Path(Constants.INPUT_FILE_PATH + "posts.csv"),
                    TextInputFormat.class, PostsMapper.class);
            MultipleInputs.addInputPath(job, new Path(Constants.INPUT_FILE_PATH + "likes.csv"),
                    TextInputFormat.class, LikesMapper.class);

            TableMapReduceUtil.initTableReducerJob(
                    String.valueOf(PostsHBaseModel.TABLE_POSTS),
                    PostLikesReducer.class,
                    job);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //for writeing output into HDFS Text file
       /*     Path outputPath = new Path(Constants.OUTPUT_FILE_PATH);
            FileOutputFormat.setOutputPath(job, outputPath);
            outputPath.getFileSystem(conf).delete(outputPath);*/
            return job;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
