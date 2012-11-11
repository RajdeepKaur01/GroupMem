package edu.uiuc.groupmessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import edu.uiuc.groupmessage.GroupMessageProtos.Member;
import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;

class MemListServerWorker extends Thread {
  MemListNode currentNode;
  Member currentMember;
  Socket sock;
  MemListServerWorker(MemListNode current_node, Socket sock) {
    super();
    this.currentNode = current_node;
    this.currentMember = current_node.getCurrentMember();
    this.sock = sock;
  }
  public void run() {
    System.out.println("MemListServerWorker is up");
    try {
      // Open I/O
      InputStream sock_in = sock.getInputStream();
      OutputStream sock_out = sock.getOutputStream();
      GroupMessage msg = GroupMessage.parseDelimitedFrom(sock_in);

      // Print the message content
      if (msg.getAction() != GroupMessage.Action.PUT_FILE && msg.getAction() != GroupMessage.Action.PUSH_FILE) {
	System.out.println(msg.toString());
      }

      // Process the message
      GroupMessage response = currentNode.processMessage(msg);
      if (response != null) {
	response.writeDelimitedTo(sock_out);
	sock_out.flush();
      }

      // Close I/O
      sock_out.close();
      sock_in.close();
      sock.close();    
    } catch(IOException ex) {
      System.out.println(ex.getMessage());
    }
    System.out.println("MemListServerWorker is down");
  }
}
