package com.example.hadoop.employee;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.logging.Logger;

public class EmployeeDrive {

    private static Logger logger = Logger.getLogger(EmployeeDrive.class.getName());
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if(remainingArgs.length !=2) {
            logger.severe("please enter the input and output path");
            System.exit(2);
        }

        Job job = new Job(configuration, "Employee health analysis");
        job.setJobName("Health check");
        job.setJarByClass(EmployeeDrive.class);
        job.setMapperClass(EmployeeMapper.class);
        job.setReducerClass(EmployeeReducer.class);
        job.setPartitionerClass(CustomPartition.class);

        job.setNumReduceTasks(2);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
        System.exit( job.waitForCompletion(true) ? 0 : 1);

    }
}
