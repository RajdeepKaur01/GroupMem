package edu.uiuc.groupmessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;

import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;
import edu.uiuc.groupmessage.GroupMessageProtos.Member;
import com.google.protobuf.ByteString;

class MemListNode {
  private Member currentMember;
  private Member portalMember;
  private Member heartbeatFrom;
  private Member heartbeatTo;
  private LinkedList< Member > memberList;
  private MemListServer server;
  private Timer heartbeatClientTimer;
  private Timer detectorTimer;
  private long heartbeatTimestamp;
  private final Logger LOGGER = Logger.getLogger(MemListNode.class.getName());
  MemListNode(Member current_member, Member portal_member) {
    currentMember = current_member;
    portalMember = portal_member;
    heartbeatFrom = null;
    heartbeatTo = null;
    memberList = new LinkedList< Member >();
    heartbeatClientTimer = null;
    detectorTimer = null;
    setHeartbeatTimestamp(-1);
    try {
      FileHandler fileTxt = new FileHandler(
	"/tmp/" + current_member.getIp() + "_" + current_member.getPort() + ".log");
      fileTxt.setFormatter(new SimpleFormatter());
      LOGGER.addHandler(fileTxt);
    } catch (IOException ex) {
      System.err.println("Log file cannot be created");
      System.exit(-1);
    }
    //LOGGER.addHandler(new ConsoleHandler());
  }

  public void runServer() {
    server = new MemListServer(this);
    server.start();
    startHeartbeatServer();
  }

  public Member getCurrentMember() {
    return currentMember;
  }

  public Member getPortalMember() {
    return portalMember;
  }

  public Member getHearbeatFrom() {
    return heartbeatFrom;
  }

  public Member getHeartbeatTo() {
    return heartbeatTo;
  }

  synchronized public void setHeartbeatTimestamp(long timestamp) {
    heartbeatTimestamp = timestamp;
  }

  synchronized public long getHeartbeatTimestamp() {
    return heartbeatTimestamp;
  }

  public GroupMessage processMessage(GroupMessage msg) {
    switch(msg.getAction()) {
    case JOIN_REQUEST:
      return handleJoinRequest(msg.getTarget());
    case RESET_MEMBERLIST:
      handleResetMemberList(msg.getMemberList());
      break;
    case TARGET_JOINS:
      handleTargetJoins(msg.getTarget());
      break;
    case TARGET_LEAVES:
      handleTargetLeaves(msg.getTarget());
      break;
    case TARGET_FAILS:
      handleTargetFails(msg.getTarget());
      break;
    case TARGET_HEARTBEATS:
      handleHeartbeats(msg.getTarget());
      break;
    case PUT_FILE:
      return handlePutFile(msg.getFileName(), msg.getFileContent());
    default:
      LOGGER.info("Unknown message action " + msg.getAction().name());
      break;
    }
    return null;
  }

  public GroupMessage handlePutFile(String file_name, ByteString file_content) {
    LOGGER.warning("Received file " + file_name + " of size " + file_content.size());
    return GroupMessage.newBuilder()
	    .setTarget(currentMember)
	    .setAction(GroupMessage.Action.FILE_OK)
	    .build();
  }

  public void handleHeartbeats(Member sender) {
    LOGGER.info("Received heartbeat from node " + memberToID(sender));
    if (heartbeatFrom != null && heartbeatFrom.equals(sender)) {
      long time = System.currentTimeMillis();
      LOGGER.info("Update "
		  + memberToID(sender)
		  + "'s heartbeat timestamp to " + time/1000);
      setHeartbeatTimestamp(time);
    }
  }

  public void handleResetMemberList(List< Member > member_list) {
    LOGGER.warning("Reset the member list");
    setMemberList(member_list);
    startHeartbeatClient();
    startFailureDetector();
    LOGGER.info(member_list.toString());
  }

  public GroupMessage handleJoinRequest(Member joiner) {
    // Log the event
    LOGGER.warning("The member " + memberToID(joiner) + " just joins.");

    // Update the memberList
    addMember(joiner);

    // Start hearbearting and failure detection
    startHeartbeatClient();
    startFailureDetector();

    // Broadcast the join message
    broadcastTargetJoin(joiner);

    // Send back the latest memberList to joiner
    return GroupMessage.newBuilder()
      .setTarget(joiner)
      .setAction(GroupMessage.Action.RESET_MEMBERLIST)
      .addAllMember(memberList)
      .build();
  }

  public void handleTargetJoins(Member joiner) {
    // Log the event
    LOGGER.warning("The member " + memberToID(joiner) + " just joins.");

    // Update the memberList
    if (!joiner.equals(currentMember)) {
      addMember(joiner);
    } else {
      LOGGER.severe("Duplicated Member Joins");
    }

    // Start hearbearting and failure detection
    startHeartbeatClient();
    startFailureDetector();
  }

  public void handleTargetLeaves(Member leaver) {
    // Log the event
    LOGGER.warning("The member " + memberToID(leaver) + " just leaves.");

    // Update the memberList
    if (!removeMember(leaver)) {
      LOGGER.severe("Somebody leaves without being on the list");
    }

    // Restart heartbeating
    startHeartbeatClient();
    startFailureDetector();
  }

  public void handleTargetFails(Member loser) {
    // Log the event
    LOGGER.warning("The member " + memberToID(loser) + " just fails.");

    // Update the memberList
    if (!removeMember(loser)) {
      LOGGER.severe("Somebody fails but he/she is not on the list");
    }

    // Restart heartbeating and failure detection
    startHeartbeatClient();
    startFailureDetector();
  }

  public void broadcastTargetJoin(Member joiner) {
    GroupMessage msg = GroupMessage.newBuilder()
      .setTarget(joiner)
      .setAction(GroupMessage.Action.TARGET_JOINS)
      .build();
    LinkedList< Member > list = new LinkedList< Member >(memberList);
    list.remove(joiner);
    list.remove(currentMember);
    broadcastMessage(msg, list);
  }

  public void broadcastTargetFail(Member loser) {
    GroupMessage msg = GroupMessage.newBuilder()
      .setTarget(loser)
      .setAction(GroupMessage.Action.TARGET_FAILS)
      .build();
    LinkedList< Member > list = new LinkedList< Member >(memberList);
    list.remove(loser);
    list.remove(currentMember);
    broadcastMessage(msg, list);
  }

  public void sendJoinRequestTo() {
    try {
      // Open I/O
      Socket sock = new Socket(portalMember.getIp(), portalMember.getPort());
      InputStream sock_in = sock.getInputStream();
      OutputStream sock_out = sock.getOutputStream();

      // Send Join Request
      GroupMessage.newBuilder()
	.setTarget(currentMember)
	.setAction(GroupMessage.Action.JOIN_REQUEST)
	.build()
	.writeDelimitedTo(sock_out);
      sock_out.flush();

      // Update the membership list
      GroupMessage msg = GroupMessage.parseDelimitedFrom(sock_in);
      processMessage(msg);

      // Close I/O
      sock_out.close();
      sock_in.close();
      sock.close();
    } catch(Exception ex) {
      System.out.println(ex.getMessage());
    }
  }

  public void broadcastMessage(GroupMessage msg, List< Member > list) {
    for (Member member : list) {
      sendMessageTo(msg, member);
    }
  }

  public void sendMessageTo(GroupMessage msg, Member receiver) {
    new Thread() {
      private GroupMessage msg;
      private Member receiver;

      public Thread initialize(GroupMessage msg, Member receiver) {
	this.msg = msg;
	this.receiver = receiver;
	return this;
      }
      public void run() {
	LOGGER.info(
	  "Send request to " + memberToID(receiver) +
	  " with Target " + memberToID(msg.getTarget()) +
	  " and Action " + msg.getAction().name()
	  );
	try {
	  Socket sock = new Socket(receiver.getIp(), receiver.getPort());
	  OutputStream sock_out = sock.getOutputStream();
	  msg.writeDelimitedTo(sock_out);
	  sock_out.flush();
	  sock_out.close();
	  sock.close();
	} catch(Exception ex) {
	  System.out.println(ex.getMessage());
	}
      }
    }.initialize(msg, receiver).start();
  }

  public void broadcastTargetLeave() {
    GroupMessage msg = GroupMessage.newBuilder()
      .setTarget(currentMember)
      .setAction(GroupMessage.Action.TARGET_LEAVES)
      .build();
    LinkedList< Member > list = new LinkedList< Member >(memberList);
    list.remove(currentMember);
    broadcastMessage(msg, list);
    stopHeartbeatClient();
    stopFailureDetector();
  }

  public void setMemberList(List< Member > member_list) {
    synchronized(memberList) {
      this.memberList = new LinkedList< Member >(member_list);
    }
    sortMemberList();
  }

  public void addMember(Member member) {
    synchronized(memberList) {
      this.memberList.add(member);
      System.out.println(memberList.toString());
    }
    sortMemberList();
  }

  public void sortMemberList() {
    synchronized(memberList) {
      // Sort the memberList by timestamp to ensure everyone has the same order
      Collections.sort(memberList, new Comparator< Member >() {
	@Override
	public int compare(Member m1, Member m2) {
	  return
	    Integer.valueOf(m1.getTimestamp()).compareTo(m2.getTimestamp());
	}
      });

      // Update the member HeartbeatFrom and HeartbeatTo
      int index = memberList.indexOf(currentMember);
      int size = memberList.size();
      if (size > 0) {
	heartbeatFrom = memberList.get((index + size - 1) % size);
	heartbeatTo = memberList.get((index + 1) % size);
      } else {
	heartbeatFrom = null;
	heartbeatTo = null;
      }
    }
  }

  public boolean removeMember(Member member) {
    boolean result;
    synchronized(memberList) {
      result = this.memberList.remove(member);
      LOGGER.info(memberList.toString());
    }
    sortMemberList();
    return result;
  }

  public static String getCurrentIp() {
    try {
      NetworkInterface nif = NetworkInterface.getByName("eth0");
      Enumeration<InetAddress> addrs = nif.getInetAddresses();
      while (addrs.hasMoreElements()) {
	InetAddress addr = addrs.nextElement();
	if (addr instanceof Inet4Address) {
	  return addr.getHostAddress();
	}
      }
      return null;
    } catch(SocketException ex) {
      System.out.println(ex.getMessage());
      System.exit(-1);
      return null;
    }
  }

  public static String memberToID(Member member) {
    StringBuilder str = new StringBuilder();
    return str.append(member.getIp()).append("_")
      .append(member.getPort()).append("_")
      .append(member.getTimestamp())
      .toString();
  }

  public String toString() {
    return currentMember.toString() + "\n" + portalMember.toString();
  }

  public void startHeartbeatServer() {
    new HeartbeatServer(this).start();
  }

  public void startHeartbeatClient() {
    // Stop the previous heartbeat if existing
    if (heartbeatClientTimer != null) {
      stopHeartbeatClient();
    }

    // Setup the heartbeatClientTimer
    LOGGER.info("Start heartbeating to " + memberToID(heartbeatTo));
    heartbeatClientTimer = new Timer("HeartbeatClient");
    heartbeatClientTimer.scheduleAtFixedRate(
      new HeartbeatClient(this, heartbeatTo), 0, 1000);
  }

  public void stopHeartbeatClient() {
    if (heartbeatClientTimer != null) {
      heartbeatClientTimer.cancel();
      heartbeatClientTimer = null;
    }
  }

  public void startFailureDetector() {
    // Stop the previous detector if existing
    if (detectorTimer != null) {
      stopFailureDetector();
    }

    // Initialize timestamp
    setHeartbeatTimestamp(System.currentTimeMillis());

    // Setup the detectorTimer
    detectorTimer = new Timer("FailureDetector");
    detectorTimer.scheduleAtFixedRate(
      new FailureDetector(this), 0, 100);
  }

  public void stopFailureDetector() {
    if (detectorTimer != null) {
      detectorTimer.cancel();
      detectorTimer = null;
      setHeartbeatTimestamp(-1);
    }
  }

  public void detectFailure() {
    if (heartbeatFrom == null) {
      return;
    }
    long current_time = System.currentTimeMillis();
    if (current_time - getHeartbeatTimestamp() > 4500) {
      LOGGER.warning("Detect failure.");
      stopFailureDetector();
      broadcastTargetFail(heartbeatFrom);
      handleTargetFails(heartbeatFrom);
    }
  }

  public static void main(String[] args) {

    // Check for insufficient arguements
    if (args.length < 3) {
      System.out.println("Usage:");
      System.out.println(
	"java edu.uiuc.groupmessage.MemListNode " +
	"<listen_port> <portal_ip> <portal_port>"
	);
      System.exit(-1);
    }

    // Process the arguments
    Member current_member = Member.newBuilder()
      .setPort(Integer.parseInt(args[0]))
      .setIp(getCurrentIp())
      .setTimestamp((int)(System.currentTimeMillis()/1000))
      .build();
    Member portal_member = Member.newBuilder()
      .setPort(Integer.parseInt(args[2]))
      .setIp(args[1])
      .build();
    MemListNode node = new MemListNode(current_member, portal_member);

    // Run server that accepts join/leave/fail request
    node.runServer();

    // Join the Group
    node.sendJoinRequestTo();

    // Handle commands
    BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
    String str;
    System.out.println("Please enter command:");
    try {
      while ((str = input.readLine()) != null) {
	if (str.equals("leave")) {
	  node.broadcastTargetLeave();
	} else if (str.equals("join")) {
	  node.sendJoinRequestTo();
	}
	System.out.println("Please enter command:");
      }
    } catch(IOException ex) {
      System.out.println(ex.getMessage());
      System.exit(-1);
    }
  }
}
