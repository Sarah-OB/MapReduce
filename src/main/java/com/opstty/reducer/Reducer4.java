package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer4 extends Reducer<Text, Text, Text, Text> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float maxHeight = 0;
        for (Text height : values) {
            if (Float.parseFloat(height.toString()) > maxHeight && height.getLength() != 0){
                //System.out.println("For key : " + key + " the value : " + Float.parseFloat(height.toString()) + " is greater than the current max: " + maxHeight);
                maxHeight = Float.parseFloat(height.toString());
            }
            /*else {
                //System.out.println("For key : " + key + " the value : " + Float.parseFloat(height.toString()) + " is not greater than the current max: " + maxHeight);
            }*/
        }
        result.set(maxHeight);
        //System.out.println(key + " " + result);
        context.write(key, new Text(String.valueOf(result)));
    }
}
