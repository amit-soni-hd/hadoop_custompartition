package com.example.hadoop.employee;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartition extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numReduceTask) {

        int partition = 0;

        int healthy_eating = Integer.parseInt(value.toString().split(",")[3]);

        if (numReduceTask != 0) {
            if (healthy_eating <= 5) {
                partition = 0;
            } else {
                partition = 1;
            }
        }
        return partition;
    }
}
