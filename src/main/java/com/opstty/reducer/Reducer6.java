package com.opstty.reducer;

import com.opstty.writable.AgeDistrict;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer6 extends Reducer<Text, AgeDistrict, Text, AgeDistrict> {
    private IntWritable ageMax = new IntWritable();

    public void reduce(Text key, Iterable<AgeDistrict> values, Context context) throws InterruptedException, IOException {
        System.out.println("Reducer");
        int ageMaxTmp = 0;
        for (AgeDistrict ad : values) {
           System.out.println(ad.toString());
            //arrTmp = ad.getArrondissement();

            //System.out.println("Pour le genre : " + key + ", l'arrondissement : " + ad.getArrondissement() + ", l'age max est : " + ageMaxTmp);*//*
        }
        //ageMax.set(ageMaxTmp);


        context.write(key, values.iterator().next());
    }

}
