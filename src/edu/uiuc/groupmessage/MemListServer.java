package edu.uiuc.groupmessage;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

class MemListServer extends Thread {
  ServerSocket serverSock;
  MemListNode currentNode;
  MemListServer(MemListNode current_node) {
    super();
    this.currentNode = current_node;
  }
  public void run() {
    System.out.println("MemListServer is up");
    try {
      serverSock = new ServerSocket(currentNode.getCurrentMember().getPort());
      Socket sock = null;
      while ((sock = serverSock.accept()) != null) {		
        MemListServerWorker worker = new MemListServerWorker(currentNode, sock);
        worker.start();
      }
      serverSock.close();
    } catch(IOException ex) {
      System.out.println(ex.getMessage());
    }
    System.out.println("MemListServer is down");
  }
}
