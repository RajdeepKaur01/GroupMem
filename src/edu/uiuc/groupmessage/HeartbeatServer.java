package edu.uiuc.groupmessage;

import java.io.IOException;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import com.google.protobuf.ByteString;

import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;

class HeartbeatServer extends Thread {
  int port;
  byte[] buf;
  MemListNode currentNode;
  DatagramSocket sock;
  HeartbeatServer(MemListNode current_node) {
    super();
    this.currentNode = current_node;
    port = this.currentNode.getCurrentMember().getPort() + 1;
    buf = new byte[128];
  }
  public void run() {
    try {
      sock = new DatagramSocket(port);
      while (true) {
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        sock.receive(packet);
        GroupMessage msg = GroupMessage.parseFrom(
          ByteString.copyFrom(buf, 0, packet.getLength())
          );
        currentNode.processMessage(msg);
      }
    } catch(IOException ex) {
      System.out.println(ex.getMessage());
    }
  }
}
