package com.opstty.writable;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AgeDistrict implements Writable {
    private final static int actualYear = 2020;
    private IntWritable age;
    private IntWritable arrondissement;

    public AgeDistrict() {
        this.age = new IntWritable();
        this.arrondissement = new IntWritable();
    }

    public AgeDistrict(IntWritable age, IntWritable arrondissement) {
        this.age = age;
        this.arrondissement = arrondissement;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        age.write(dataOutput);
        arrondissement.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        age.readFields(dataInput);
        arrondissement.readFields(dataInput);
    }

    public void setAge(int annee) {
        this.age = new IntWritable(actualYear - annee);
    }

    public void setArrondissement(IntWritable arrondissement) {
        this.arrondissement = arrondissement;
    }

    public IntWritable getAge() {
        return age;
    }

    public IntWritable getArrondissement() {
        return arrondissement;
    }

    /*public int compareTo(AgeDistrict x) {
        if (this.getArrondissement() == x.getArrondissement() && this.getAge() > x.getAge()) {
            return 1;
        }
        else if (this.getArrondissement() == x.getArrondissement() && this.getAge() < x.getAge()) {
            return -1;
        }
        else if (this.getArrondissement() == x.getArrondissement() && this.getAge() == x.getAge()) {
            return 0;
        }
        return -9;
    }*/

    public String toString(){
        return "Je viens de l'arrondissement : " + this.getArrondissement() + " et j'ai : " + this.getAge() + " ans.";
    }
}

