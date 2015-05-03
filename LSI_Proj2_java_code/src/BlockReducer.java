import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class BlockReducer extends Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		
		Double damp=0.85;
		Long totalnodes=MainClass.numNodes;
		Double sumPR=0.0;
		Double oldPR=0.0;
		//String outEdges="";
		HashMap<String,Double> nodeToOldPRmap = new HashMap<String,Double>();
		HashMap<String,String> incomingInternalEdgesMap = new HashMap<String,String>();

		HashMap<String,String> nodeToOutEdgesMap = new HashMap<String,String>();
		
		//HashMap<String,Double> PRfromBoundaryEdgesMap = new HashMap<String,Double>();

		//HashMap<String,Double> internalNodePRmap = new HashMap<String,Double>();
		HashMap<String,Double> PRfromBoundarymap = new HashMap<String,Double>();
		HashMap<String,Double> OldPRmap = new HashMap<String,Double>();

		HashMap<String,Double> newPageRankMap = new HashMap<String,Double>();

		String blockID = key.toString();
		
		for(Text val:values)
		{
			String input=val.toString();
			String inputarr[]=input.split("\\s+");
		
			if(inputarr[0].equals(MainClass.NODEINFO)){
				//received: Key:BlockID, Value:NODEINFO nodeID nodePR outgoingEgdes
				Double PR = 0.0;
				String nodeID = inputarr[1];
				PR = Double.parseDouble(inputarr[2]);
				nodeToOldPRmap.put(nodeID, PR);
				OldPRmap.put(nodeID, PR);
				if(inputarr.length == 4){
					nodeToOutEdgesMap.put(nodeID, inputarr[3]);	
					String edgesArray[] = inputarr[3].split(":");
					
					for(String blockEdgePair : edgesArray){
						String blockEdgeEntry[] = blockEdgePair.split(",");
						String neighborNode = blockEdgeEntry[1];
						if(blockEdgeEntry[0].equals(blockID)){
							String incomingEdges = "";
							if(incomingInternalEdgesMap.containsKey(neighborNode)){
								incomingEdges = incomingInternalEdgesMap.get(neighborNode);
								incomingEdges = incomingEdges + nodeID + ",";
							}else{
								incomingEdges = nodeID + ",";
							}
							incomingInternalEdgesMap.put(neighborNode, incomingEdges);
						}
					}
				}else{
					nodeToOutEdgesMap.put(nodeID,"");					
				}
			}else{
				//received Key:blockID, value:NodeID + " " + BlockId of incoming edge + " " + PR contribution from that node
				if(blockID.equals(inputarr[1])){
					//Internal edge
					
					/*String nodeID = inputarr[0];
					Double PR = 0.0;
					if(internalNodePRmap.containsKey(nodeID)){
						PR = internalNodePRmap.get(nodeID);
						PR = PR + Double.parseDouble(inputarr[2]);					
					}else{
						PR = Double.parseDouble(inputarr[2]);					
					}
					internalNodePRmap.put(nodeID, PR);
					*/
				}else{
					String nodeID = inputarr[0];
					Double PR = 0.0;
					if(PRfromBoundarymap.containsKey(nodeID)){
						PR = PRfromBoundarymap.get(nodeID);
						PR = PR + Double.parseDouble(inputarr[2]);					
					}else{
						PR = Double.parseDouble(inputarr[2]);					
					}
					PRfromBoundarymap.put(nodeID, PR);
				}
				
			}
		}
		
		/*
		void IterateBlockOnce(B) {
		    for( v ∈ B ) { NPR[v] = 0; }
		    for( v ∈ B ) {
		        for( u where <u, v> ∈ BE ) {
		            NPR[v] += PR[u] / deg(u);
		        }
		        for( u, R where <u,v,R> ∈ BC ) {
		            NPR[v] += R;
		        }
		        NPR[v] = d*NPR[v] + (1-d)/N;
		    }
		    for( v ∈ B ) { PR[v] = NPR[v]; }
		}
		*/
		int count = 0;
		while(count < 10){
			count++;
			for(String nodeID : nodeToOldPRmap.keySet()){
				newPageRankMap.put(nodeID, 0.0);
			}
			for(String nodeID : nodeToOldPRmap.keySet()){

				Double npr = newPageRankMap.get(nodeID);

				String incomingEdges = incomingInternalEdgesMap.get(nodeID);
				String incomingEdgesArray[] = incomingEdges.split(",");
				for(String vertex : incomingEdgesArray){
					String outEdges = nodeToOutEdgesMap.get(vertex);
					int denom = 1;
					if(outEdges.equals("")){
						denom = 1;
					}else{
						denom  = outEdges.split(":").length;
					}
					npr = npr + nodeToOldPRmap.get(vertex)/denom;
				}
				
				Double rankFromBC = 0.0;
				if(PRfromBoundarymap.containsKey(nodeID)){
					rankFromBC = PRfromBoundarymap.get(nodeID);
				}
				
				npr = npr + rankFromBC;
				// NPR[v] = d*NPR[v] + (1-d)/N;
				npr = (1-damp)/totalnodes + damp*npr;
				newPageRankMap.put(nodeID,npr);

			}
			for(String nodeID : nodeToOldPRmap.keySet()){
				nodeToOldPRmap.put(nodeID, newPageRankMap.get(nodeID));
			}
		}
		
		Double sumOldPR = 0.0, sumNewPR = 0.0;
		for(Double val : OldPRmap.values()){
			sumOldPR = sumOldPR + val;
		}
		for(Double val : newPageRankMap.values()){
			sumNewPR = sumNewPR + val;
		}
		long residual=(long) Math.abs(((sumOldPR-sumNewPR)/sumNewPR)*MainClass.precision);
		context.getCounter(MainClass.MRCounter.RESIDUAL).increment(residual);
		// add code to add residual to hadoop counter
		
		//String emitString=key.toString()+";"+newPR.toString()+";"+outEdges.split(",").length+";"+outEdges;
		//String emitString=newPR.toString()+" "+outEdges.split(",").length+" "+outEdges;
		//context.write(key, new Text(emitString));
		
	}
		
	
}
