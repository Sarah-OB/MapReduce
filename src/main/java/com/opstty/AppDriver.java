package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class, "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtqTrees", DistrictsTrees.class, "A map/reduce program that counts the districts containing trees.");
            programDriver.addClass("existingSpecies", ExistingSpecies.class, "A map/reduce program that show all the existing species.");
            programDriver.addClass("treesBySpecies", TreesBySpecies.class, "A map/reduce program that counts the number of trees by species.");
            programDriver.addClass("maxHeight", MaxHeightPerSpecieOfTree.class, "A map/reduce program that calculate the maximum height per specie of tree.");
            programDriver.addClass("sortTrees", SortTrees.class, "A map/reduce program that sort the trees height from the smallest to the largest.");
            programDriver.addClass("oldestTree", DistrictOldestTree.class, "A map/reduce program that show the oldest tree.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
