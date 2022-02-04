package de.tuda.dmdb.operator.exercise;

import de.tuda.dmdb.net.TCPServer;
import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.operator.ReceiveBase;
import de.tuda.dmdb.storage.AbstractRecord;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Implementation of receive operator
 *
 * @author melhindi
 */
public class Receive extends ReceiveBase {


  /**
   * Constructor of Receive
   *
   * @param child Child operator used to process next calls, usually SendOperator
   * @param numPeers Number of peer nodes that have to finish processing before operator finishes
   * @param listenerPort Port on which to bind receive server
   * @param nodeId Own nodeId, used for debugging
   */
  public Receive(Operator child, int numPeers, int listenerPort, int nodeId) {
    super(child, numPeers, listenerPort, nodeId);
  }

  @Override
  public void open() {
    // TODO implement this method
    // init local cache
    // create tcp server to listen for in-coming connections
    localCache = new ConcurrentLinkedQueue<AbstractRecord>();
    try {
      receiveServer = new TCPServer(listenerPort,localCache,finishedPeers);
      receiveServer.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
    // call open on child after starting receive server, so that sendOperator can
    // connect
    child.open();
  }

  @Override
  public AbstractRecord next() {
    // TODO implement this method
    // get next element from send operator and remote
    AbstractRecord r = child.next();  // child是send operator
    if (r != null) {
      return r;
    }
    while (finishedPeers.get()!=numPeers||!localCache.isEmpty()){
      AbstractRecord rr = localCache.poll();
      System.out.println("fp"+finishedPeers.get());
      System.out.println("numPeers"+numPeers);
      if(rr!=null){
        return rr;
      }
    }

    return null;
  }

  @Override
  public void close() {
    // TODO implement this method
    receiveServer.stopServer();
    child.close();
  }
}
