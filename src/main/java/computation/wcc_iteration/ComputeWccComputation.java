
package computation.wcc_iteration;

import computation.WccMasterCompute;
import static computation.WccMasterCompute.ISOLATED_COMMUNITY;

import messages.*;
import utils.ArrayPrimitiveWritable;
import utils.NeighborUtils;
import utils.Debug;
import vertex.WccVertexData;
import aggregators.CommunityAggregator;
import aggregators.CommunityAggregatorData;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.Map;
import java.util.Set;
//import java.util.LinkedList;
import java.util.ArrayList;
import java.util.HashSet;

import java.io.IOException;

import org.apache.giraph.utils.MemoryUtils;

public class ComputeWccComputation extends BasicComputation<
  IntWritable, WccVertexData, NullWritable, ArrayPrimitiveWritable> {

  // TODO: Refactor this stuff into superclass
  private boolean finished;
  private int stepsToDo;
  private int currentStep;

  @Override
  public void preSuperstep() {
    stepsToDo = ((IntWritable)
        getAggregatedValue(WccMasterCompute.NUMBER_OF_WCC_STEPS)).get();
    currentStep = ((IntWritable)
        getAggregatedValue(WccMasterCompute.INTERPHASE_STEP)).get();
    finished = (currentStep == stepsToDo);
  }

  @Override
  public void postSuperstep() {
    //TODO: Put in superclass
    double freeMemory = MemoryUtils.freeMemoryMB()/1000; // Mem in gigs
    double freeNotInHeap = (MemoryUtils.maxMemoryMB() - MemoryUtils.totalMemoryMB())/1000;
    aggregate(WccMasterCompute.MIN_MEMORY_AVAILABLE, new DoubleWritable(freeMemory + freeNotInHeap));
    if (!finished) {
      aggregate(WccMasterCompute.REPEAT_PHASE, new BooleanWritable(true));
    }
  }

  @Override
  public void compute(
      Vertex<IntWritable, WccVertexData, NullWritable> vertex,
      Iterable<ArrayPrimitiveWritable> messages) {

    WccVertexData vData = vertex.getValue();

    if (currentStep == 0) {
      vData.setCommunityT(0);
      vData.setCommunityVt(0);
    } else {
      updateCommunityTriangleCounts(vertex, messages);
    }

    if (finished && vData.getCommunity() != WccMasterCompute.ISOLATED_COMMUNITY) {
      if (vData.getCommunityT() % 2 != 0) {
        System.out.println("ComputeWccComputation: communityT should be even!");
        System.out.println("vid: " + vertex.getId());
        System.exit(-1);
      }
      // Because every triangle is counted twice - once for each other node in the triangle
      vData.setCommunityT(vData.getCommunityT()/2);

      // Publish the aggregates needed for wcc estimation in the next step
      publishCommunityAggregates(vData);
    } else {
      sendNeighborsInCommunityToNeighborsInCommunity(vertex);
    }
  }

  private void sendNeighborsInCommunityToNeighborsInCommunity(
      Vertex<IntWritable, WccVertexData, NullWritable> vertex) {
    
    int[] nicArr = (int[]) NeighborUtils.getNeighborsInCommunity(vertex).get();
    
    ArrayPrimitiveWritable multiNeighborsInCommunity =
      NeighborUtils.getMultiNeighborsInCommunity(vertex);

    ArrayList<IntWritable> targets = new ArrayList();

    for (int i = currentStep; i < nicArr.length; i += stepsToDo) {
      targets.add(new IntWritable(nicArr[i]));
    }
    //sendMessageToMultipleEdges(targets.iterator(), neighborsInCommunity);
    
    //modify: neighborsInCommunity + id
    //modify to get neighbor id and neighbor in community
    sendMessageToMultipleEdges(targets.iterator(), 
          neighborsInCommunityWithSelfId(vertex.getId(), multiNeighborsInCommunity));
  }

  private void updateCommunityTriangleCounts(
      Vertex<IntWritable, WccVertexData, NullWritable> vertex, 
      Iterable<ArrayPrimitiveWritable> messages) {

    WccVertexData vData = vertex.getValue();
    int communityT = vData.getCommunityT();
    int communityVt = vData.getCommunityVt();
    
    for (ArrayPrimitiveWritable m : messages) {
      //int[] neighborNeighbors = (int[]) m.get();
      //int numCommonNeighbors = NeighborUtils.countCommonNeighbors(vertex, neighborNeighbors);
      int numCommonNeighbors = countCommonNeighbors(vertex, m); // compatible multi-edges
      
      if (numCommonNeighbors != 0) {
        communityT += numCommonNeighbors;
        communityVt += 1;
      }
    }
    vData.setCommunityT(communityT);
    vData.setCommunityVt(communityVt);
  }

  /**
   *  Initialize community aggregates
   *    Publish community name to SIZE aggregate
   *    Publish # neighbors in your community to
   *     EDGE_DENSITY aggregate
   *    Publish # neighbors not in your community to
   *     BOUNDARY_EDGES aggregate
   */
  private void publishCommunityAggregates(WccVertexData vData) {

    int internalEdges = 0, borderEdges = 0;

    for (Map.Entry<Writable, Writable> nc : vData.getNeighborCommunityMap().entrySet()) {
      //IntWritable neighbor    = (IntWritable) nc.getKey();
      //IntWritable neighborComm  = (IntWritable) nc.getValue();
      //if (neighborComm.get() == vData.getCommunity()) internalEdges++;
      //else borderEdges++;
      // modify to suit the multi-edge
      int[] commEdges = (int[]) ((ArrayPrimitiveWritable) nc.getValue()).get();
      if (commEdges[0] == vData.getCommunity()) { internalEdges += commEdges[1]; }
      else { borderEdges += commEdges[1]; }
    }

    MapWritable vertexCommStats = new MapWritable();
    vertexCommStats.put(
        new IntWritable(vData.getCommunity()), 
        new CommunityAggregatorData(1, internalEdges, borderEdges));
    aggregate(WccMasterCompute.COMMUNITY_AGGREGATES, vertexCommStats);
  }
  
  private ArrayPrimitiveWritable neighborsInCommunityWithSelfId(IntWritable id, ArrayPrimitiveWritable nic) {
    int[] nicArr = (int[]) nic.get();
    int[] nicWithId = new int[nicArr.length + 1];
    for (int i = 0; i < nicArr.length; i++) {
      nicWithId[i] = nicArr[i];
    }
    nicWithId[nicArr.length] = id.get();
    
    return new ArrayPrimitiveWritable(nicWithId);
  }
  
  private int countCommonNeighbors(Vertex<IntWritable, WccVertexData, NullWritable> vertex, ArrayPrimitiveWritable nnWritable) {
    // neighborsInCommunity + id
    // modify to get neighbor id and neighbor in community, in order to use NeighborUtils.countCommonDuplicateNeighbors method
    int[] nn = (int[]) nnWritable.get();
    int[] nnArr = new int[nn.length-1];
    for (int i = 0; i < nn.length-1; i++) {
      nnArr[i] = nn[i];
    }
    int source = nn[nn.length-1];
    return NeighborUtils.countCommonDuplicateNeighbors(vertex, source, nnArr);
  }
}
