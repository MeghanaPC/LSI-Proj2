

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

public class OutputReporting {
	
	 public static void main(String[] args) throws Exception {
		   
		 ArrayList<Integer> blocknumberlist=new ArrayList<Integer>();
		  // blockNumberArray = new int[68];
		   int index = 0;
		   int prev = 0;
		   blocknumberlist.add(0);
		   blocknumberlist.add(1);
		   BufferedReader blockFileReader = new BufferedReader(new FileReader("blocks.txt"));
		   String line = null;
		   while((line = blockFileReader.readLine()) != null){
			   if(index > 0){
				   prev = blocknumberlist.get(index-1);
			   }
			   Integer num=prev + Integer.parseInt(line.trim());
			   blocknumberlist.add(num);
			   blocknumberlist.add(num+1);
			   
			   index++;
		   }
		   
		   
		   blockFileReader.close();
		   
		   BufferedReader reader = new BufferedReader(new FileReader("gaur_val"));
		   BufferedWriter writer=new BufferedWriter(new FileWriter("Report_gaur.txt"));
		   String line1=null;
		   HashMap<String,String> map=new HashMap<String,String>();
		   ArrayList<Integer> dummy=new ArrayList<Integer>();
		   while((line1=reader.readLine())!= null)
		   {
			   String input[]=line1.split(":");
			   Integer nodeID=Integer.parseInt(input[0]);
			   String output="";
			   if(blocknumberlist.contains(nodeID))
			   {
				   dummy.add(nodeID);
				   map.put(nodeID.toString(), input[1]);
				   //output=nodeID+" : " +input[1];
				  // writer.write(output);
				   //writer.newLine();
			   }
			   
		   }
		   Collections.sort(dummy);
		   for(Integer i:dummy)
		   {
			   writer.write(i+" : "+map.get(i.toString()));
			   writer.newLine();
		   }
		   
		   reader.close();
		   writer.close();
		   
	 
	 }
}
