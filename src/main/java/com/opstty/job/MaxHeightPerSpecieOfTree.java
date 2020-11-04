package com.opstty.job;

import com.opstty.mapper.Mapper4;
import com.opstty.reducer.Reducer4;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxHeightPerSpecieOfTree {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Nombre d'arguments incorrects");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Maximum height per specie of tree");
        job.setJarByClass(MaxHeightPerSpecieOfTree.class);
        job.setMapperClass(Mapper4.class);
        job.setCombinerClass(Reducer4.class);
        job.setReducerClass(Reducer4.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
