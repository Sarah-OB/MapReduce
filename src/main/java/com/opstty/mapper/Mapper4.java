package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Mapper4 extends Mapper<Object, Text, Text, Text> {
    private Text genre = new Text();
    private Text hauteur = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String csv = value.toString();
        String[] arrLine = csv.split(";");

        if (arrLine[0].equals("GEOPOINT") || arrLine[1].equals("ARRONDISSEMENT") || arrLine[2].equals("GENRE") || arrLine[3].equals("ESPECE") || arrLine[4].equals("FAMILLE") || arrLine[5].equals("ANNEE PLANTATION") || arrLine[6].equals("HAUTEUR") || arrLine[7].equals("CIRCONFERENCE") || arrLine[8].equals("ADRESSE") || arrLine[9].equals("NOM COMMUN") || arrLine[10].equals("VARIETE") || arrLine[11].equals("OBJECTID") || arrLine[12].equals("NOM_EV")){
            return;
        }

        genre.set(arrLine[2]);
        //hauteur = new FloatWritable(Float.parseFloat(arrLine[6]));
        hauteur.set(arrLine[6]);

        if (!(hauteur.toString().equals("\t") || hauteur.toString().equals(" ") || hauteur.toString().equals(""))) {
            context.write(genre,hauteur);
        }
    }

}

