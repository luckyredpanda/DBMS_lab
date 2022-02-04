package de.tuda.dmdb.net;

import de.tuda.dmdb.storage.AbstractRecord;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;

/**
 * Implementation of a Client component based on TCP. This class is used in the send operator to
 * send records to other peers in a parallel setup. The TCPClient uses an outgoing object stream to
 * send (serialized) objects to the server side
 *
 * @author melhindi
 */
public class TCPClient {

  protected Socket socket = null; // The client socket
  protected ObjectOutputStream objectOutputStream = null; // Stream to write to server
  protected int sourceNodeId = 0; // the nodeId where the client resides
  protected int destinationNodeId = 0; // the nodeId where the client sends to

  /**
   * Constructor of TCPClient
   *
   * @param host The remote host (IP/domain name) to which to connect
   * @param port The port of the remote host to which to connect
   * @param sourceNodeId The id of the node for the TCPClient is created (source of message)
   * @param destinationNodeId The id of the node to which this TCPClient connects (destination of a
   *     message)
   * @throws UnknownHostException Thrown when IP/domain name could not be resolved
   * @throws IOException Thrown on socket issues
   */
  public TCPClient(String host, int port, int sourceNodeId, int destinationNodeId)
      throws UnknownHostException, IOException {
    int maxRetries = 3;
    int waitingTime = 1; // seconds
    int retryCounter = 0;
    this.sourceNodeId = sourceNodeId;
    this.destinationNodeId = destinationNodeId;
    boolean connectionSuccess = false; // indicate success to avoid retry
    while (!connectionSuccess && retryCounter < maxRetries) {
      try {
        System.out.println(
            "TCPClient-"
                + this.sourceNodeId
                + "-"
                + this.destinationNodeId
                + ": Connecting to "
                + host
                + " on "
                + port);
        this.socket = new Socket(host, port);
        connectionSuccess = true;
      } catch (ConnectException e) {
        System.err.println(
            "TCPClient-"
                + this.sourceNodeId
                + "-"
                + this.destinationNodeId
                + ": The following error occured: "
                + e.getMessage());
        // wait and retry...
        ++retryCounter;
        System.out.println(
            "TCPClient-"
                + this.sourceNodeId
                + "-"
                + this.destinationNodeId
                + ": Connection not successful, will retry to connect to "
                + host
                + " on port "
                + port);
        try {
          System.out.println(
              "TCPClient-"
                  + this.sourceNodeId
                  + "-"
                  + this.destinationNodeId
                  + ": Waiting for "
                  + waitingTime
                  + " seconds");
          Thread.sleep(waitingTime * 1000);
        } catch (InterruptedException e1) {
          System.err.println(
              "TCPClient-"
                  + this.sourceNodeId
                  + "-"
                  + this.destinationNodeId
                  + ": Got the following error:");
          e1.printStackTrace();
        }
        // retry
      }
    }
    if (!connectionSuccess || this.socket == null) {
      System.err.println(
          "TCPClient-"
              + this.sourceNodeId
              + "-"
              + this.destinationNodeId
              + ": No connection could be established!");
      throw new RuntimeException(
          "TCPClient-"
              + this.sourceNodeId
              + "-"
              + this.destinationNodeId
              + ": Unable to connect! No connection established!");
    }

    try {
      this.objectOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
      System.out.println(
          "TCPClient-"
              + this.sourceNodeId
              + "-"
              + this.destinationNodeId
              + ": Connected successfully to "
              + host
              + ":"
              + port);
    } catch (SocketException e) {
      System.err.println(
          "TCPClient-"
              + this.sourceNodeId
              + "-"
              + this.destinationNodeId
              + ": Connection closed by remote!");
      throw new RuntimeException(
          "TCPClient-"
              + this.sourceNodeId
              + "-"
              + this.destinationNodeId
              + ": Unable to connect! No connection established!",
          e);
    }
  }

  /**
   * Sends a record to the a receive server through an object output stream
   *
   * @param record - Record to transfer to server
   * @return - Returns true on success, else false
   */
  public boolean sendRecord(AbstractRecord record) {
    System.out.println(
        "TCPClient-"
            + this.sourceNodeId
            + "-"
            + this.destinationNodeId
            + ": sending record "
            + record
            + " to server "
            + this.socket.getRemoteSocketAddress());
    try {
      this.objectOutputStream.writeObject(record);
    } catch (SocketException e) {
      e.printStackTrace();
      System.exit(1);
      return false;
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  /**
   * Close connection to the remote server
   *
   * @return - Returns true on success, else false
   */
  public boolean close() {
    if (this.socket == null) {
      return true;
    }
    System.out.println(
        "TCPClient-"
            + this.sourceNodeId
            + "-"
            + this.destinationNodeId
            + ": Closing connection to "
            + this.socket.getRemoteSocketAddress());
    try {
      this.socket.close();
    } catch (IOException e) {
      System.err.println(
          "TCPClient-"
              + this.sourceNodeId
              + "-"
              + this.destinationNodeId
              + ": not close Socket to "
              + this.socket.getRemoteSocketAddress());
      e.printStackTrace();
      return false;
    }
    return true;
  }

  /**
   * Getter method for object output stream member
   *
   * @return - Returns the object output stream member
   */
  public ObjectOutputStream getObjectOutputStream() {
    return objectOutputStream;
  }

  /**
   * Setter method for object output stream member
   *
   * @param objectOutputStream - ObjectOutputStrem to use for record transfer
   */
  public void setObjectOutputStream(ObjectOutputStream objectOutputStream) {
    this.objectOutputStream = objectOutputStream;
  }
}
