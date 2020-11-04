package com.opstty.job;

import com.opstty.mapper.Mapper6;
import com.opstty.reducer.Reducer6;
import com.opstty.writable.AgeDistrict;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistrictOldestTree {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Nombre d'arguments incorrects");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "District containing the oldest Tree");
        job.setJarByClass(DistrictOldestTree.class);
        job.setMapperClass(Mapper6.class);
        job.setCombinerClass(Reducer6.class);
        job.setReducerClass(Reducer6.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AgeDistrict.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
