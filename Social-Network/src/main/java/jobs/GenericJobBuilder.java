package jobs;


import org.apache.hadoop.mapreduce.Job;

public interface GenericJobBuilder {
    Job buildJob();
}
