import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/* newwwwwwwwwwwwww*/
public class BlockMapper extends Mapper<LongWritable,Text,Text,Text>{
	//file format nodeid pagerank #outgoingEdges outgoingEdgeList (delimited by ,)
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String input=value.toString();
		String inputarr[]=input.trim().split("\\s+");
		String nodeID=inputarr[0];
		String blockID = blockIDFromNodeID(nodeID);
		Double nodePR=new Double(inputarr[1]);
		Integer nodeDegree=new Integer(inputarr[2]);
		String nodeEdges="";
		if(inputarr.length==4)
		{
			nodeEdges=inputarr[3];
		}
		 
		String edgeListString = "";
		
		//System.out.println("input passed to mapper-------"+input);
		if(nodeEdges!="")
		{
			Double newPR=nodePR/nodeDegree;    //won't be 0 if nodeEdges!=""
			String edgeList[]=nodeEdges.split(",");
			for(String edgenode:edgeList)
			{
				String neighborBlockID = blockIDFromNodeID(edgenode);
				edgeListString += neighborBlockID + ":" + edgenode + ",";
				if(!neighborBlockID.equals(blockID)){
					context.write(new Text(neighborBlockID),new Text(edgenode + " " + blockID + " " + newPR.toString()));
				}
			}
			edgeListString = edgeListString.substring(0,edgeListString.length()-1);

		} 
		
		//edgeListString = edgeListString.substring(0,edgeListString.length()-1);
		context.write(new Text(blockID), new Text(BlockedMainClass.NODEINFO + " " + nodeID + " " + nodePR + " " + edgeListString));

		
	}

	private String blockIDFromNodeID(String edgenode) {
		//Dummy implementation:
		/*
		if(edgenode.equals("0") || edgenode.equals("1")){
			return "0";
		}else if(edgenode.equals("2")){
			return "1";
		}else{
			return "2";
		}
		*/
		int nodeNum = Integer.parseInt(edgenode);
		Integer blockNum = 0;
		for( int i = 0; i < BlockedMainClass.blockNumberArray.length; i++){
			if(nodeNum <  BlockedMainClass.blockNumberArray[i]){
				blockNum = i;
				break;
			}
		}
		
		return blockNum.toString();
		/*
		int nodeNum = Integer.parseInt(edgenode);
		
		Integer quotient = nodeNum/BlockedMainClass.blockSize;
		int blockNum = BlockedMainClass.blockNumberArray[quotient];
				
		int prevBlockLimit = 0;
		
		if(quotient > 0){
			prevBlockLimit = BlockedMainClass.blockNumberArray[quotient-1];
		}
		
		if(nodeNum < blockNum){
			if(nodeNum > prevBlockLimit){
				return quotient.toString();
			}else{
				quotient = quotient-1;
				return quotient.toString();
			}
		}else{
			quotient = quotient+1;
			return quotient.toString();
		}
		*/
	}
}
