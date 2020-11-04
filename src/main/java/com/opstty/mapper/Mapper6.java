package com.opstty.mapper;

import com.opstty.writable.AgeDistrict;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper6 extends Mapper<Object, Text, Text, AgeDistrict> {
    private Text arrondissement = new Text();
    private Text anneePlantation = new Text();
    private AgeDistrict ageDistrict = new AgeDistrict();
    private Text genre = new Text();

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String csv = value.toString();
        String[] arrLine = csv.split(";");

        if (arrLine[0].equals("GEOPOINT") || arrLine[1].equals("ARRONDISSEMENT") || arrLine[2].equals("GENRE") || arrLine[3].equals("ESPECE") || arrLine[4].equals("FAMILLE") || arrLine[5].equals("ANNEE PLANTATION") || arrLine[6].equals("HAUTEUR") || arrLine[7].equals("CIRCONFERENCE") || arrLine[8].equals("ADRESSE") || arrLine[9].equals("NOM COMMUN") || arrLine[10].equals("VARIETE") || arrLine[11].equals("OBJECTID") || arrLine[12].equals("NOM_EV")){
            return;
        }

        arrondissement.set(arrLine[1]);
        anneePlantation.set(arrLine[5]);
        genre.set(arrLine[2]);

        /*System.out.println(arrondissement.toString());
        System.out.println(anneePlantation.toString());
        System.out.println(genre.toString());*/

        if (!((anneePlantation.toString().equals("\t") || anneePlantation.toString().equals(" ") || anneePlantation.toString().equals("")) && anneePlantation.getLength() == 0)) {
            ageDistrict.setAge(Integer.parseInt(anneePlantation.toString()));
            ageDistrict.setArrondissement(new IntWritable(Integer.parseInt(arrondissement.toString())));
            System.out.println(genre.toString() + " " + ageDistrict.toString());
            context.write(genre, ageDistrict);
        }
    }
}
