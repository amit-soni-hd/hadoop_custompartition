package com.example.hadoop.employee;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.logging.Logger;

public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Logger logger = Logger.getLogger(EmployeeMapper.class.getName());

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] employee_data = value.toString().split(",");

        if (employee_data.length == 6) {
            String group = employee_data[1];
            context.write(new Text(group), value);
        } else {
            logger.info("wrong employee data");
        }
    }
}
