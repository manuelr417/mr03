package edu.uprm.cse.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountBirthByYearSexMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //StringTokenizer strTok = new StringTokenizer(value.toString(), ",");
        String cols[] = value.toString().split(",");
        // get the state name, located in column
        String year = cols[2];
        String sex = cols[1];
        String newKey = year + "-" + sex;
        context.write(new Text(newKey), new IntWritable(1));

    }
}
