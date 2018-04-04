
package utils;

import vertex.WccVertexData;
import utils.Debug;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Arrays;

public class NeighborUtils {
  // Do not instantiate
  private NeighborUtils() {}

   /**
   * Gets an array of IntWritables to be passed in to the
   * PreprocessingMessage constructor. TODO: for now uses the first element to
   * be contain the source vertex id. Later on should just modify the
   * PreprocessingMessage class
   * @param vertex Vertex
   * @return An array containing the ids of the vertex's neighbors
   */
  public static ArrayPrimitiveWritable getNeighborsWritableArray(
      Vertex<IntWritable, WccVertexData, NullWritable> vertex) {

    int[] neighbors = new int[vertex.getNumEdges()];

    int i = 0;
    for (Edge<IntWritable, NullWritable> e : vertex.getEdges()) {
      neighbors[i] = e.getTargetVertexId().get();
      i++;
    }
    return new ArrayPrimitiveWritable(neighbors);
  }
  
  /**
   * new method
   * Gets an iterator of IntWritables to be passed in to the
   * PreprocessingMessage constructor. TODO: for now uses the first element to
   * be contain the source vertex id. Later on should just modify the
   * PreprocessingMessage class
   * @param vertex Vertex
   * @return An iterator containing the ids of the vertex's neighbors
   */
  public static Iterator<IntWritable> getNeighborsWritableIterator(
      Vertex<IntWritable, WccVertexData, NullWritable> vertex) {
    
    ArrayList<IntWritable> neighbors = new ArrayList<IntWritable>();
    
    for (Edge<IntWritable, NullWritable> e : vertex.getEdges()) {
      int neighbor = e.getTargetVertexId().get();
      // i dont know why got true when compared e.getTargetVertexId() directly,
      // even target vertex id the is different,
      // so only take out int and re-put in IntWritable
      if (!neighbors.contains(new IntWritable(neighbor))) {
        neighbors.add(new IntWritable(neighbor));
      }
    }
    return neighbors.iterator();
  }  

  public static int countCommonNeighbors(
      Vertex<IntWritable, WccVertexData, NullWritable> vertex,
      int[] neighborNeighbors) {

    int numCommonNeighbors = 0;
    IntWritable nn = new IntWritable();
    for (int i = 0; i < neighborNeighbors.length; i++) {
      nn.set(neighborNeighbors[i]);
      if (vertex.getEdgeValue(nn) != null) {
        numCommonNeighbors++;
      }
    }
    return numCommonNeighbors;
  }
  
  public static int countCommonDuplicateNeighbors(
      Vertex<IntWritable, WccVertexData, NullWritable> vertex,
      int neighborId, int[] neighborNeighbors) {
    int[] myNeighbors = (int[]) vertex.getValue().getNeighbors().get();
    if (myNeighbors.length == 0) {
      ArrayPrimitiveWritable neighborsInCommunity = NeighborUtils.getNeighborsInCommunity(vertex);
      myNeighbors = (int[]) neighborsInCommunity.get();
    }
    int numCommonNeighbors = 0;
    int edgesInTwoVertices = 0;
    for (int i : myNeighbors) {
      if (i == neighborId) { edgesInTwoVertices++; }
      for (int j : neighborNeighbors) {
        if (i == j) { numCommonNeighbors++; }
      }
    }
    
    return edgesInTwoVertices * numCommonNeighbors;
  }

  /**
   *  Use the neighborCommunityMap of the vertex to build a list of its neighbors
   *  in the same community as the vertex and wrap it in an
   *  ArrayPrimitiveWritable
   */
  public static ArrayPrimitiveWritable getNeighborsInCommunity(
      Vertex<IntWritable, WccVertexData, NullWritable> vertex) {

    WccVertexData vData = vertex.getValue();
    MapWritable ncm = vData.getNeighborCommunityMap();

    ArrayList<Integer> neighborsInCommunity = new ArrayList<Integer>();
    for(Map.Entry<Writable, Writable> entry : ncm.entrySet()) {
      int id = ((IntWritable) entry.getKey()).get();
      int[] commEdges = (int[]) ((ArrayPrimitiveWritable) entry.getValue()).get();
      if (commEdges[0] == vData.getCommunity()) {
        neighborsInCommunity.add(id);
      }
    }
    
    int[] arr = new int[neighborsInCommunity.size()];
    for (int i = 0; i < neighborsInCommunity.size(); i++) {
      arr[i] = neighborsInCommunity.get(i);
    }
    return new ArrayPrimitiveWritable(arr);
    /*
    // Count neighbors in community
    int numNeighborsInCommunity = 0;
    for(Map.Entry<Writable, Writable> entry : ncm.entrySet()) {
      int neighborCommunity = ((IntWritable) entry.getValue()).get();
      if (neighborCommunity == vData.getCommunity()) {
        numNeighborsInCommunity++;
      }
      int[] commEdges = (int[]) ((ArrayPrimitiveWritable) entry.getValue()).get();
      if (commEdges[0] == vData.getCommunity()) {
        numNeighborsInCommunity++;
      }
    }

    int[] neighborsInCommunity = new int[numNeighborsInCommunity];

    int i = 0;
    for(Map.Entry<Writable, Writable> entry : ncm.entrySet()) {
      int neighborCommunity = ((IntWritable) entry.getValue()).get();
      if (neighborCommunity == vData.getCommunity()) {
        IntWritable id = (IntWritable) entry.getKey();
        neighborsInCommunity[i] = id.get();
        i++;
      }
    }
    return new ArrayPrimitiveWritable(neighborsInCommunity);
    */
  }
  
  /**
   *  Get neighbors in same community
   *  Add same neighbor several times if there are multi-edges between it
   *  
   */
  public static ArrayPrimitiveWritable getMultiNeighborsInCommunity(
      Vertex<IntWritable, WccVertexData, NullWritable> vertex) {
    
    int[] neighborsInCommunity = (int[]) getNeighborsInCommunity(vertex).get();
    Arrays.sort(neighborsInCommunity);
    
    ArrayList<Integer> neighbors = new ArrayList<Integer>();
    
    for (Edge<IntWritable, NullWritable> e : vertex.getEdges()) {
      int neighbor = e.getTargetVertexId().get();
      if (Arrays.binarySearch(neighborsInCommunity, neighbor) >= 0) {
        neighbors.add(neighbor);
      }
    }
    
    int[] arr = new int[neighbors.size()];
    for (int i = 0; i < neighbors.size(); i++) {
      arr[i] = neighbors.get(i);
    }
    return new ArrayPrimitiveWritable(arr);
  }
}