package com.opstty.mapper;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Mapper1 extends Mapper<Object, Text, Text, NullWritable> {
    //private final static IntWritable one = new IntWritable(1);
    private Text geopoint  = new Text();
    private Text genre = new Text();
    private Text espece = new Text();
    private Text famille = new Text();
    private Text anneePlantation = new Text();
    private Text hauteur = new Text();
    private Text circonference = new Text();
    private Text adresse = new Text();
    private Text nom_commun = new Text();
    private Text variete = new Text();
    private Text objectID = new Text();
    private Text nomEv = new Text();
    private Text arrondissement = new Text();


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String csv = value.toString();
        String[] arrLine = csv.split(";");

        if (arrLine[0].equals("GEOPOINT") || arrLine[1].equals("ARRONDISSEMENT") || arrLine[2].equals("GENRE") || arrLine[3].equals("ESPECE") || arrLine[4].equals("FAMILLE") || arrLine[5].equals("ANNEE PLANTATION") || arrLine[6].equals("HAUTEUR") || arrLine[7].equals("CIRCONFERENCE") || arrLine[8].equals("ADRESSE") || arrLine[9].equals("NOM COMMUN") || arrLine[10].equals("VARIETE") || arrLine[11].equals("OBJECTID") || arrLine[12].equals("NOM_EV")){
            return;
        }

        geopoint  = new Text(arrLine[0]);
        arrondissement = new Text(arrLine[1]);
        genre = new Text(arrLine[2]);
        espece = new Text(arrLine[3]);
        famille = new Text(arrLine[4]);
        anneePlantation = new Text(arrLine[5]);
        hauteur = new Text(arrLine[6]);
        circonference = new Text(arrLine[7]);
        adresse = new Text(arrLine[8]);
        nom_commun = new Text(arrLine[9]);
        variete = new Text(arrLine[10]);
        objectID = new Text(arrLine[11]);
        nomEv = new Text(arrLine[12]);


        context.write(arrondissement, NullWritable.get());
    }

}

