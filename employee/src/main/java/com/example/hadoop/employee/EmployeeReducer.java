package com.example.hadoop.employee;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EmployeeReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int max_salary = Integer.MIN_VALUE;
        String value = "";

        for (Text employee : values) {
            int salary = Integer.parseInt(employee.toString().split(",")[5]);
            if(salary > max_salary) {
                max_salary = salary;
                value = employee.toString();
            }
        }
        context.write(NullWritable.get(), new Text(value));
    }
}
