package random;

import java.awt.List;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import javax.sound.sampled.Line;


public class PreProcessingInput {
	
	private static final String INPUT_PATH = "/Users/nikhil/Documents/workspace/LSI-Project2/src/edges.txt";
	private static final String OUTPUT_PATH = "/Users/nikhil/Documents/workspace/LSI-Project2/src/processed.txt";
	private static final String FINAL_PATH = "/Users/nikhil/Documents/workspace/LSI-Project2/src/final.txt";
	private static final String BLOCK_INPUT = "/Users/nikhil/Documents/workspace/LSI-Project2/src/blocks.txt";
	private static final String BLOCK_PATH = "/Users/nikhil/Documents/workspace/LSI-Project2/src/aggregate.txt";
	
//	private static final String OUTPUT_PATH = "/Users/nikhil/Documents/workspace/LSI-Project2/src/test-data/processed.txt";
//	private static final String FINAL_PATH = "/Users/nikhil/Documents/workspace/LSI-Project2/src/test-data/final.txt";
	
	private static final String NETID_NUMBER = "366";
	private static double rejectMin = 0.0;
	private static double rejectLimit = 0.0;
	private static String initialPageRank = "0.000001459";
	static ArrayList<Long> blockLimits = new ArrayList<Long>();
	
	/*
	* Computes ranges based on netID
	*/
	public static void computeRange(String netIDNumber) {
		String reversedNumber = new StringBuilder(netIDNumber).reverse().toString();
		double fromNetID = Double.parseDouble(reversedNumber);
		fromNetID = fromNetID * 0.001;
		rejectMin = 0.9 * fromNetID;
		rejectLimit = rejectMin + 0.01;
	}
	
	private static boolean selectInputLine(double x) {
		return ( ((x >= rejectMin) && (x < rejectLimit)) ? false : true );
	}
	
	/*
	* Reads the block file and computes aggregate of block ranges
	*/
	public static void processBlockFile() throws IOException {
		
		FileReader reader  = new FileReader(new File(BLOCK_INPUT));
		BufferedReader bufferedReader = new BufferedReader(reader);
		FileWriter writer = new FileWriter(new File(BLOCK_PATH));
		BufferedWriter bufferedWriter = new BufferedWriter(writer);
		String line = "";
		long runningCount = 0;
		while ((line = bufferedReader.readLine())!= null) {
			long currentLimit = Long.parseLong(line.substring(1,6).trim());
			runningCount += currentLimit;
			bufferedWriter.write(runningCount + "\n");
			blockLimits.add(runningCount);
		}
		bufferedReader.close();
		bufferedWriter.close();
	}
	
	/*
	* Returns block ranges for specified node ID
	*/
	public static int getBlockID(int nodeID) {

		Long[] blockArray = new Long[blockLimits.size()];
		blockArray = blockLimits.toArray(blockArray);
		
		for(int i = 0; i < blockArray.length ; i++){
			int j = i + 1;
				if(nodeID > blockArray[i] && nodeID < blockArray[j])
					return j+1;
			
		}
		if (nodeID < blockArray[0]) {
			return 1;
		}
		
		if (nodeID > blockArray[blockArray.length - 1]) {
			return blockArray.length;
		}
		
		return 0;
	}
	
	/*
	* Reads the edges file and rejects edges - needs to be run first
	*/
	public static void processLine() throws IOException {
		
		FileReader reader  = new FileReader(new File(INPUT_PATH));
		BufferedReader bufferedReader = new BufferedReader(reader);
		FileWriter writer = new FileWriter(new File(OUTPUT_PATH));
		BufferedWriter bufferedWriter = new BufferedWriter(writer);
		String line = "";
		
		computeRange(NETID_NUMBER);
		System.out.println(rejectMin);
		System.out.println(rejectLimit);
		
		while ((line = bufferedReader.readLine())!= null) {
			double randomDouble = Double.parseDouble(line.substring(1, 11));
			int sourceNode = Integer.parseInt(line.substring(12,18).trim());
			int destinationNode = Integer.parseInt(line.substring(19,25).trim());
			if(selectInputLine(randomDouble)) {
				bufferedWriter.write(line.substring(11) + "\n");
			}
		}
		bufferedReader.close();
		bufferedWriter.close();
	}
	
	/*
	* Must be run after select line - returns final file but without missing nodes
	*/
	public static void finalProcessing() throws IOException {
		
		FileReader reader  = new FileReader(new File(OUTPUT_PATH));
		BufferedReader bufferedReader = new BufferedReader(reader);
		FileWriter writer = new FileWriter(new File(FINAL_PATH));
		BufferedWriter bufferedWriter = new BufferedWriter(writer);
		String line = "";
		
		
		int lastNode = 0; //need to set it to initial source node value
		int outgoingCount = 0;
		String toBeReturned = "";
		ArrayList<Integer> outgoingList = new ArrayList<Integer>();
		StringBuilder outgoingBuilder = new StringBuilder();
		
		while ((line = bufferedReader.readLine())!= null) {
			int sourceNode = Integer.parseInt(line.substring(1,7).trim());
			int destinationNode = Integer.parseInt(line.substring(8).trim());
			if (sourceNode == lastNode) {
				outgoingCount += 1;
				if (outgoingList.contains(destinationNode) == false) {
					outgoingBuilder.append("," + line.substring(8).trim());
					outgoingList.add(destinationNode);
				}
//				toBeReturned = getBlockID(sourceNode)  + " " + sourceNode + " " + initialPageRank + " " +  + outgoingCount + " " + outgoingBuilder.toString().substring(1) + "\n";
				toBeReturned = sourceNode + " " + initialPageRank + " " + outgoingCount + " " + outgoingBuilder.toString().substring(1) + "\n";
			}
			else {
				bufferedWriter.write(toBeReturned);
				
				outgoingBuilder.setLength(0);
				outgoingBuilder.append("," + line.substring(8).trim());
				outgoingCount = 1;
				lastNode = sourceNode;
				
				outgoingList.clear();
				outgoingList.add(destinationNode);
//				toBeReturned = getBlockID(sourceNode)  + " " + sourceNode + " " + initialPageRank + " " +  + outgoingCount + " " + outgoingBuilder.toString().substring(1) + "\n";
				toBeReturned = sourceNode + " " + initialPageRank + " " + outgoingCount + " " + outgoingBuilder.toString().substring(1) + "\n";
			}
//			System.out.println(outgoingList);
		}
		bufferedWriter.write(toBeReturned);
		bufferedReader.close();
		bufferedWriter.close();
	}
	
	/*
	* random block id function to be used for random partition
	*/
	public static long randomBlockID(long nodeID) {
		Long nodeIDLong = new Long(nodeID);
		return nodeIDLong.hashCode() % 68;
	}
	
	public static void main(String[] args) throws IOException {
		processBlockFile();
//		processLine();
		finalProcessing();
	}
}
