package cn.edu.nju.pasa.graph.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Scanner;

/**
 * Generate a batch update file from an edge list.
 * If an edge in the edge list appears in the initial data graph, the update flag of the edge is -1.
 * Otherwise, the update flag is 1.
 * Arg1: path of the forward adj file of the initial data graph.
 * Arg2: path of the edge list file.
 * Arg3: path of the output update file.
 */
public class GenerateBatchUpdateFileFromEdgeList {

    public static IntArrayList convertToIntList(String str) {
        IntArrayList blankPoses = new IntArrayList();
        blankPoses.add(-1);
        int p = str.indexOf(" ");
        while (p >= 0) {
            blankPoses.add(p);
            p = str.indexOf(" ", p + 1);
        }
        IntArrayList ints = new IntArrayList();
        for (int i = 1; i < blankPoses.size(); i++) {
            String substr = str.substring(blankPoses.getInt(i - 1) + 1, blankPoses.getInt(i));
            int x = Integer.parseInt(substr);
            ints.add(x);
        }
        int x = Integer.parseInt(str.substring(blankPoses.getInt(blankPoses.size() - 1) + 1));
        ints.add(x);
        return ints;
    }

    public static void main(String args[]) throws FileNotFoundException {
        String initForwardAdjFilePath = args[0];
        String edgeListFilePath = args[1];
        String outputFilePath = args[2];
        LongOpenHashSet edgeSetOfInitGraph = new LongOpenHashSet();
        System.err.println("Load init graph file " + initForwardAdjFilePath + ".");
        Scanner scanner = new Scanner(new File(initForwardAdjFilePath));
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            IntArrayList list = convertToIntList(line);
            int vid = list.getInt(0);
            for (int i = 1; i < list.size(); i++) {
                int nvid = list.getInt(i);
                long signature = (long)vid << 32L | nvid;
                edgeSetOfInitGraph.add(signature);
            }
        }
        scanner.close();
        System.err.println("Long init graph done! |E|=" + edgeSetOfInitGraph.size() + ".");
        System.err.println("Process edge list file " + edgeListFilePath + ".");
        scanner = new Scanner(new File(edgeListFilePath));
        PrintWriter writer = new PrintWriter(outputFilePath);
        long numAddEdges = 0;
        long numDeleteEdges = 0;
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            int pos = line.indexOf(" ");
            int src = Integer.parseInt(line.substring(0, pos));
            int dst = Integer.parseInt(line.substring(pos + 1));
            long signature = (long)src << 32L | dst;
            if (edgeSetOfInitGraph.contains(signature)) {
                numDeleteEdges++;
                writer.println(src + " " + dst + " -1");
            } else {
                numAddEdges++;
                writer.println(src + " " + dst + " 1");
            }
        }
        scanner.close();
        writer.close();
        System.err.println("Process edge list done. numAddEdges = " + numAddEdges
                + ", numDeleteEdges = " + numDeleteEdges);
    }
}
