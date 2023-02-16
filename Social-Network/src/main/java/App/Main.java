package App;

import HbaseTables.CreatePostsTable;
import combiners.PostCountAndAvgLikesCombiner;
import jobs.GenericJobBuilder;
import jobs.JoinPostsAndLikesJob;
import jobs.PostCountAndAvgLikesJob;
import mappers.LikesMapper;
import mappers.PostCountAndAvgLikesMapper;
import mappers.PostsMapper;
import models.PostsHBaseModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducers.PostCountAndAvgLikesReducer;
import reducers.PostLikesReducer;
import utilities.Constants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //create Hbase table posts
        CreatePostsTable.createTable();

        Configuration conf1 = new Configuration();
        conf1.set("hbase.zookeeper.quorum", Constants.CLUSTER_MACHINES);

        //Map reduce job for joining posts and Likes
        Job job1 = Job.getInstance(conf1, "Join Posts and Likes");
        job1.setJarByClass(Main.class);
        MultipleInputs.addInputPath(job1, new Path(Constants.INPUT_FILE_PATH + "posts.csv"),
                TextInputFormat.class, PostsMapper.class);
        MultipleInputs.addInputPath(job1, new Path(Constants.INPUT_FILE_PATH + "likes.csv"),
                TextInputFormat.class, LikesMapper.class);

        TableMapReduceUtil.initTableReducerJob(
                String.valueOf(PostsHBaseModel.TABLE_POSTS),
                PostLikesReducer.class,
                job1);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.waitForCompletion(true);

        //for writeing output into HDFS Text file
       /*     Path outputPath = new Path(Constants.OUTPUT_FILE_PATH);
            FileOutputFormat.setOutputPath(job, outputPath);
            outputPath.getFileSystem(conf).delete(outputPath);*/


        // Map reduce job for computing metrices
        Configuration conf2 = new Configuration();
        conf2.set("hbase.zookeeper.quorum", Constants.CLUSTER_MACHINES);

        Job job2 = Job.getInstance(conf2, "Post count and average likes");
        job2.setJarByClass(Main.class);
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
                job2);
        job2.setCombinerClass(PostCountAndAvgLikesCombiner.class);//combiner class
        job2.setReducerClass(PostCountAndAvgLikesReducer.class);  // reducer class

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        Path outputPath = new Path(Constants.OUTPUT_FILE_PATH);
        FileOutputFormat.setOutputPath(job2, outputPath);
        outputPath.getFileSystem(conf2).delete(outputPath);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}