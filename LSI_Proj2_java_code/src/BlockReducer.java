import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/* newwwwwwwwwwwwww*/
public class BlockReducer extends Reducer<Text, Text, Text, NullWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Double damp = 0.85;
		Long totalnodes = BlockedMainClass.numNodes;

		HashMap<String, Double> nodeToOldPRmap = new HashMap<String, Double>();
		HashMap<String, String> incomingInternalEdgesMap = new HashMap<String, String>();

		HashMap<String, String> nodeToOutEdgesMap = new HashMap<String, String>();

		// HashMap<String,Double> internalNodePRmap = new
		// HashMap<String,Double>();
		HashMap<String, Double> PRfromBoundarymap = new HashMap<String, Double>();
		HashMap<String, Double> OldPRmap = new HashMap<String, Double>();

		HashMap<String, Double> newPageRankMap = new HashMap<String, Double>();

		String blockID = key.toString();

		for (Text val : values) {
			String input = val.toString();
			String inputarr[] = input.trim().split("\\s+");

			if (inputarr[0].equals(BlockedMainClass.NODEINFO)) {
				// received: Key:BlockID, Value:NODEINFO nodeID nodePR
				// outgoingEgdes
				Double PR = 0.0;
				String nodeID = inputarr[1];
				PR = Double.parseDouble(inputarr[2]);
				nodeToOldPRmap.put(nodeID, PR);
				OldPRmap.put(nodeID, PR);
				if (inputarr.length == 4) {
					nodeToOutEdgesMap.put(nodeID, inputarr[3]);
					String edgesArray[] = inputarr[3].split(",");

					for (String blockEdgePair : edgesArray) {
						String blockEdgeEntry[] = blockEdgePair.split(":");
						String neighborNode = blockEdgeEntry[1];
						if (blockEdgeEntry[0].equals(blockID)) {
							String incomingEdges = "";
							if (incomingInternalEdgesMap
									.containsKey(neighborNode)) {
								incomingEdges = incomingInternalEdgesMap
										.get(neighborNode);
								incomingEdges = incomingEdges + nodeID + ",";
							} else {
								incomingEdges = nodeID + ",";
							}
							incomingInternalEdgesMap.put(neighborNode,
									incomingEdges);
						}
					}
				} else {
					nodeToOutEdgesMap.put(nodeID, "");
				}
			} else {
				// received Key:blockID, value:NodeID + " " + BlockId of
				// incoming edge + " " + PR contribution from that node
				if (blockID.equals(inputarr[1])) {
					// Internal edge

					/*
					 * String nodeID = inputarr[0]; Double PR = 0.0;
					 * if(internalNodePRmap.containsKey(nodeID)){ PR =
					 * internalNodePRmap.get(nodeID); PR = PR +
					 * Double.parseDouble(inputarr[2]); }else{ PR =
					 * Double.parseDouble(inputarr[2]); }
					 * internalNodePRmap.put(nodeID, PR);
					 */
				} else {
					String nodeID = inputarr[0];
					Double PR = 0.0;
					if (PRfromBoundarymap.containsKey(nodeID)) {
						PR = PRfromBoundarymap.get(nodeID);
						PR = PR + Double.parseDouble(inputarr[2]);
					} else {
						PR = Double.parseDouble(inputarr[2]);
					}
					PRfromBoundarymap.put(nodeID, PR);
				}

			}
		}

		/*
		 * void IterateBlockOnce(B) { for( v ∈ B ) { NPR[v] = 0; } for( v ∈ B )
		 * { for( u where <u, v> ∈ BE ) { NPR[v] += PR[u] / deg(u); } for( u, R
		 * where <u,v,R> ∈ BC ) { NPR[v] += R; } NPR[v] = d*NPR[v] + (1-d)/N; }
		 * for( v ∈ B ) { PR[v] = NPR[v]; } }
		 */
		int count = 0;
		int numIter=0;
		
		while (count <15) {
			++numIter;
			++count;
			for (String nodeID : nodeToOldPRmap.keySet()) {
				newPageRankMap.put(nodeID, 0.0);
			}
			for (String nodeID : nodeToOldPRmap.keySet()) {

				Double npr = newPageRankMap.get(nodeID);

				String incomingEdges = incomingInternalEdgesMap.get(nodeID);
				if (incomingEdges != null) {
					String incomingEdgesArray[] = incomingEdges.split(",");
					for (String vertex : incomingEdgesArray) {
						String outEdges = nodeToOutEdgesMap.get(vertex);
						int denom = 1;
						if (outEdges.equals("")) {
							denom = 1;
						} else {
							denom = outEdges.split(",").length;
						}
						npr = npr + nodeToOldPRmap.get(vertex) / denom;
					}
				}

				Double rankFromBC = 0.0;
				if (PRfromBoundarymap.containsKey(nodeID)) {
					rankFromBC = PRfromBoundarymap.get(nodeID);
				}

				npr = npr + rankFromBC;
				// NPR[v] = d*NPR[v] + (1-d)/N;
				npr = (1 - damp) / totalnodes + damp * npr;
				newPageRankMap.put(nodeID, npr);

			}
			/* ***************** */
			Double sum_blockRes = 0.0;
			for (String node : nodeToOldPRmap.keySet()) {
				Double newPR_block = newPageRankMap.get(node);
				Double oldPR_block = nodeToOldPRmap.get(node);
				Double temp=Math.abs(oldPR_block - newPR_block);
				sum_blockRes = sum_blockRes + (temp/newPR_block) ;
			}
			
			Double residual_block=sum_blockRes/(double)nodeToOldPRmap.keySet().size();
			if(residual_block<=BlockedMainClass.epsilon)
			{
				break;
			}
			/* **************** */
			
			
			for (String nodeID : newPageRankMap.keySet()) {
				nodeToOldPRmap.put(nodeID, newPageRankMap.get(nodeID));
			}
		}

		Double sumOfResiduals = 0.0;
		for (String node : OldPRmap.keySet()) {
			Double newPR = newPageRankMap.get(node);
			Double oldPR = OldPRmap.get(node);
			Double temp1=Math.abs(oldPR - newPR);
			sumOfResiduals = sumOfResiduals + (temp1/newPR) ;
		}
		/*
		for (Double val : OldPRmap.values()) {
			sumOldPR = sumOldPR + val;
		}
		for (Double val : newPageRankMap.values()) {
			sumNewPR = sumNewPR + val;
		}
		long residual = (long) Math.abs(((sumOldPR - sumNewPR) / sumNewPR)
				* BlockedMainClass.precision);*/
		long residual = (long) Math.abs(sumOfResiduals
				* BlockedMainClass.precision);
		context.getCounter(BlockedMainClass.MRCounter.RESIDUAL).increment(residual);
		context.getCounter(BlockedMainClass.MRCounter.AVERAGE_ITER).increment(numIter);
		// add code to add residual to hadoop counter
		// file format blockid nodeid pagerank #outgoingEdges outgoingEdgeList
		// (delimited by ,)

		for (String nodeID : newPageRankMap.keySet()) {
			String outEdges = nodeToOutEdgesMap.get(nodeID);
			String edgesArray[];
			Integer degree = 0;
			String neighborNodeString = "";
			if (!outEdges.equals("")) {
				edgesArray = outEdges.split(",");

				for (String blockEdgePair : edgesArray) {
					String blockEdgeEntry[] = blockEdgePair.split(":");
					String neighborNode = blockEdgeEntry[1];
					neighborNodeString += neighborNode + ",";
				}
				degree = edgesArray.length;
			}
			String fileRecord = nodeID + " " + newPageRankMap.get(nodeID) + " "
					+ degree + " " + neighborNodeString;
			context.write(new Text(fileRecord), NullWritable.get());
		}
 
	}

}
