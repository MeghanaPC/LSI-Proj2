import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import random.*;

public class AddMissingNodes {

	private static final String OUTPUT_PATH = "/Users/nikhil/Documents/workspace/LSI-Project2/src/All-filled-in.txt";
	private static final String FINAL_PATH = "/Users/nikhil/Documents/workspace/LSI-Project2/src/final.txt";
	private static String initialPageRank = "0.000001459";
	private static long limit = 685230;
	
	
	/*
	* Fills in the missing nodes - run AFTER preprocessing input has finished
	*/
	public static void fillInRest() throws IOException{
		
		FileReader reader  = new FileReader(new File(FINAL_PATH));
		BufferedReader bufferedReader = new BufferedReader(reader);
		FileWriter writer = new FileWriter(new File(OUTPUT_PATH));
		BufferedWriter bufferedWriter = new BufferedWriter(writer);
		String line = "";
		long prevNodeNumber = -1;
		long currentNodeNumber = 0;
		long bridge = 0;
		while ((line = bufferedReader.readLine())!= null) {
			currentNodeNumber = Long.parseLong(line.split(" ")[0]);
			if (currentNodeNumber != (prevNodeNumber + 1)) {
				bridge = prevNodeNumber+1;
				while(bridge < currentNodeNumber) {
//					bufferedWriter.write(PreProcessingInput.getBlockID((int)bridge) + " " + bridge + " " + initialPageRank + " " + 0 + " " + "\n");
					bufferedWriter.write(bridge + " " + initialPageRank + " " + 0 + " " + "\n");
					bridge += 1;
				}
			}
			bufferedWriter.write(line + "\n");
			prevNodeNumber = currentNodeNumber;
		}
		bridge = currentNodeNumber + 1;
		while(bridge <= limit) {
//			bufferedWriter.write(PreProcessingInput.getBlockID((int)bridge) + " " + bridge + " " + initialPageRank + " " + 0 + " " + "\n");
			bufferedWriter.write(bridge + " " + initialPageRank + " " + 0 + " " + "\n");
			bridge += 1;
		}
			
		bufferedReader.close();
		bufferedWriter.close();
	}
	
	public static void main(String[] args) throws IOException {
		PreProcessingInput.processBlockFile();
		fillInRest();
	}

}
