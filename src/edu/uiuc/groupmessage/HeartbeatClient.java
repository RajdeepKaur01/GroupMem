package edu.uiuc.groupmessage;

import java.io.IOException;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import java.util.TimerTask;

import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;
import edu.uiuc.groupmessage.GroupMessageProtos.Member;

class HeartbeatClient extends TimerTask {
  Member target;
  MemListNode currentNode;
  DatagramSocket sock;
  InetAddress address;
  HeartbeatClient(MemListNode current_node, Member target) {
    super();
    this.currentNode = current_node;
    this.target = target;
  }
  public void run() {
    try {
      sock = new DatagramSocket();
      byte[] buf = GroupMessage.newBuilder()
	.setTarget(currentNode.getCurrentMember())
	.setAction(GroupMessage.Action.TARGET_HEARTBEATS)
	.build()
	.toByteArray();
      address = InetAddress.getByName(target.getIp());
      if (address == null) {
	System.out.println("Wrong IP address of heartbeat target");
      }
      sock.send(new DatagramPacket(
	buf,
	buf.length,
	address,
	target.getPort() + 1)
      );
    } catch(IOException ex) {
      System.out.println(ex.getMessage());
    }
  }
}
