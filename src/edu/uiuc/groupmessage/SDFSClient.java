package edu.uiuc.groupmessage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import java.net.Socket;
import java.net.UnknownHostException;

import java.util.Arrays;

import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;
import edu.uiuc.groupmessage.GroupMessageProtos.Member;

class SDFSClient extends Thread {
  private Member master;

  SDFSClient(String masterIP, int port){
    this.master = Member.newBuilder()
      .setPort(port)
      .setIp(masterIP)
      .build();
  }

  public void deleteFile(String sdfs_name) {
    GroupMessage send_msg, rcv_msg;
    if (sdfs_name == null) {
      return;
    }
    // Prepare for GET_FILE_LOCATION message
    send_msg = GroupMessage.newBuilder()
      .setTarget(master)
      .setAction(GroupMessage.Action.GET_FILE_LOCATION)
      .setFileName(sdfs_name)
      .build();

    // send the message to the master and check the response
    rcv_msg = sendMessage(master, send_msg);
    if (rcv_msg.getAction() != GroupMessage.Action.FILE_LOCATION) {
      System.out.println("Received Unknown message action " + rcv_msg.getAction().name());
      return;
    }

    // Prepare for the DELETE_FILE message
    Member target = rcv_msg.getTarget();
    System.out.println("Deleting file at " + target.getIp() + "_" + target.getPort());
    send_msg = GroupMessage.newBuilder()
      .setTarget(target)
      .setAction(GroupMessage.Action.DELETE_FILE)
      .setFileName(sdfs_name)
      .build();

    // upload the file to the file location
    rcv_msg = sendMessage(rcv_msg.getTarget(), send_msg);
    // check the response
    switch (rcv_msg.getAction()) {
    case FILE_OK:
      System.out.println("Successfully deleted the file " + sdfs_name);
      break;
    case FILE_ERROR:
      System.out.println("Error deleting the file " + sdfs_name);
      break;
    case FILE_NOT_EXIST:
      System.out.println("Error - File does not exist " + sdfs_name);
      break;
    case FILE_LOCATION:
    default:
      System.out.println("Received Unknown action " + rcv_msg.getAction().name());
      break;
    }
  }

  public void eraseFile(String sdfs_name) {
    GroupMessage send_msg, rcv_msg;
    if (sdfs_name == null) {
      return;
    }

    // Prepare for the ERASE_FILE message
    System.out.println("Erasing file with prefix " + sdfs_name);
    send_msg = GroupMessage.newBuilder()
      .setTarget(master)
      .setAction(GroupMessage.Action.ERASE_FILE)
      .setFileName(sdfs_name)
      .build();

    // send the erase message
    rcv_msg = sendMessage(master, send_msg);

    // check the response
    switch (rcv_msg.getAction()) {
    case FILE_OK:
      System.out.println("Successfully erased the file " + sdfs_name);
      break;
    case FILE_ERROR:
      System.out.println("Error erasing the file " + sdfs_name);
      break;
    case FILE_NOT_EXIST:
      System.out.println("Error - File does not exist " + sdfs_name);
      break;
    case FILE_LOCATION:
    default:
      System.out.println("Received Unknown action " + rcv_msg.getAction().name());
      break;
    }
  }

  public void renameFile(String prefix) {
    GroupMessage send_msg, rcv_msg;
    if (prefix == null) {
      return;
    }

    // Prepare for the ERASE_FILE message
    System.out.println("Rename file with prefix " + prefix);
    send_msg = GroupMessage.newBuilder()
      .setTarget(master)
      .setAction(GroupMessage.Action.RENAME_FILE_REQUEST)
      .setFileName(prefix)
      .build();

    // send the erase message
    rcv_msg = sendMessage(master, send_msg);

    // check the response
    switch (rcv_msg.getAction()) {
    case FILE_OK:
      System.out.println("Successfully erased the file " + prefix);
      break;
    case FILE_ERROR:
      System.out.println("Error erasing the file " + prefix);
      break;
    case FILE_NOT_EXIST:
      System.out.println("Error - File does not exist " + prefix);
      break;
    case FILE_LOCATION:
    default:
      System.out.println("Received Unknown action " + rcv_msg.getAction().name());
      break;
    }
  }

  public void listFileWithPrefix(String prefix) {
    
    if (prefix == null) {
      return;
    }

    // Prepare the request message
    GroupMessage send_msg = GroupMessage.newBuilder()
      .setTarget(master)
      .setAction(GroupMessage.Action.LIST_FILE_WITH_PREFIX)
      .setFileName(prefix)
      .build();

    // Get the result
    GroupMessage rcv_msg = sendMessage(master, send_msg);
    if (rcv_msg.getAction() != GroupMessage.Action.FILE_LIST) {
      System.out.println("Failed to list files with prefix " + prefix);
      return;
    }

    // Print out the result
    for (String name : rcv_msg.getFileList()) {
      System.out.println(name);
    }
  }

  public void putFile(String local_name, String sdfs_name) {
    GroupMessage send_msg, rcv_msg;
    if (local_name == null || sdfs_name == null) {
      return;
    }

    // Check if the local file exist
    File local_file = new File(local_name);
    if (!local_file.exists()) {
      System.out.println("The local file " + local_name + " does not exist.");
      return;
    }

    // Prepare for GET_FILE_LOCATION message
    send_msg = GroupMessage.newBuilder()
      .setTarget(master)
      .setAction(GroupMessage.Action.GET_FILE_LOCATION)
      .setFileName(sdfs_name)
      .build();

    // send the message to the master and check the response
    rcv_msg = sendMessage(master, send_msg);
    if (rcv_msg.getAction() != GroupMessage.Action.FILE_LOCATION && rcv_msg.getAction() != GroupMessage.Action.FILE_NOT_EXIST) {
      System.out.println("Received Unknown message action " + rcv_msg.getAction().name());
      return;
    }

    // Send the file to the server returned by the master
    Member target = rcv_msg.getTarget();
    FileClient client = new FileClient(target, sdfs_name, local_file, GroupMessage.Action.PUT_FILE_VALUE);  
    client.start();
    try {
      client.join();
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
    }

    // check the response
    switch (client.getResult().getAction()) {
    case FILE_OK:
      System.out.println("Successfully sent the file " + local_name);
      break;
    case FILE_ERROR:
      System.out.println("Error sending the file " + local_name);
      break;
    default:
      System.out.println("Received Unknown action " + rcv_msg.getAction().name());
      break;
    }
  }

  public void getFile(String sdfs_name, String local_name) {
    GroupMessage send_msg, rcv_msg;
    if (local_name == null || sdfs_name == null) {
      return;
    }

    // Prepare for GET_FILE_LOCATION message
    send_msg = GroupMessage.newBuilder()
      .setTarget(master)
      .setAction(GroupMessage.Action.GET_FILE_LOCATION)
      .setFileName(sdfs_name)
      .build();

    // send the GET_FILE_LOCATION message to the master
    rcv_msg = sendMessage(master, send_msg);
    if (rcv_msg.getAction() != GroupMessage.Action.FILE_LOCATION) {
      System.out.println("File does not exist in SDFS");
      return;
    }

    // Start to get the file from the returned node
    Member target = rcv_msg.getTarget();
    System.out.println("Recieving file from" + target.getIp() + "_" + target.getPort());
    FileClient client = new FileClient(target, sdfs_name, new File(local_name), GroupMessage.Action.GET_FILE_VALUE);  
    client.start();
    try {
      client.join();
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
    }

    // check the response
    switch (client.getResult().getAction()) {
    case FILE_OK:
      System.out.println("Successfully received the file " + local_name);
      break;
    default:
      System.out.println("Error receiving the file " + local_name);
      break;
    }
  }

  class SDFSClientWorker extends Thread {
    private Member target;
    private GroupMessage send_msg;
    private GroupMessage rcv_msg;
    SDFSClientWorker(Member target, GroupMessage send_msg) {
      this.target = target;
      this.send_msg = send_msg;
    }

    public GroupMessage getRcvMsg() {
      return rcv_msg;
    }

    public void run() {
      try {
	Socket sock = new Socket(target.getIp(), target.getPort());
	InputStream sock_in = sock.getInputStream();
	OutputStream sock_out = sock.getOutputStream();
	send_msg.writeDelimitedTo(sock_out);
	sock_out.flush();
	rcv_msg = GroupMessage.parseDelimitedFrom(sock_in);
	sock_out.close();
	sock_in.close();
	sock.close();
      } catch (UnknownHostException ex) {
	System.out.println(ex.getMessage());
      } catch (IOException ex) {
	System.out.println(ex.getMessage());
      }
    }
  }

  private GroupMessage sendMessage(Member target, GroupMessage msg) {
    SDFSClientWorker worker = new SDFSClientWorker(target, msg);
    worker.start();
    try {
      worker.join();
    } catch (InterruptedException ex) {
      System.out.println(ex.getMessage());
    }
    return worker.getRcvMsg();
  }

  public void runCommand(String[] tokens) {
    long startTime = System.currentTimeMillis();
    if (tokens[0].equals("put") && tokens.length == 3) {
      putFile(tokens[1], tokens[2]);
    } else if (tokens[0].equals("get") && tokens.length == 3) {
      getFile(tokens[1], tokens[2]);
    } else if (tokens[0].equals("delete") && tokens.length == 2) {
      deleteFile(tokens[1]);
    } else if (tokens[0].equals("erase") && tokens.length == 2) {
      eraseFile(tokens[1]);
    } else if (tokens[0].equals("rename") && tokens.length == 2) {
      renameFile(tokens[1]);
    } else if (tokens[0].equals("list") && tokens.length == 2) {
      listFileWithPrefix(tokens[1]);
    } else {
      System.out.println("Invalid command!");
    }
    long endTime = System.currentTimeMillis();
    //System.out.println("Time elapsed: "+ (endTime-startTime) + " milli seconds");
  }

  public static void main(String[] args) throws FileNotFoundException {
    if (args.length < 2) {
      System.out.println("Usage: java edu.uiuc.groupmessage.SDFSClient <master IP> <master port>");
      System.out.println("Usage: java edu.uiuc.groupmessage.SDFSClient <master IP> <master port> put <local_file> <remote_file>");
      System.out.println("Usage: java edu.uiuc.groupmessage.SDFSClient <master IP> <master port> get <remote_file> <local_file>");
      System.out.println("Usage: java edu.uiuc.groupmessage.SDFSClient <master IP> <master port> delete <remote_file>");
      System.out.println("Usage: java edu.uiuc.groupmessage.SDFSClient <master IP> <master port> list <name_prefix>");
      System.exit(-1);
    }
    BufferedReader command_in = null;
    SDFSClient client = new SDFSClient(args[0], Integer.parseInt(args[1]));

    if(args.length == 2) {
      // Interactive Mode
      command_in = new BufferedReader(new InputStreamReader(System.in));			
      System.out.println("Please enter command:");			
      String str;
      try {
        while (!(str = command_in.readLine()).equals("exit")) {
          String[] tokens = str.split(" ");
          client.runCommand(tokens);
          System.out.println("Please enter command:");			
        }
      } catch (IOException ex) {
        System.out.println(ex.getMessage());
      }
    } else {
      // Command line Mode
      client.runCommand(Arrays.copyOfRange(args, 2, args.length));
    }
  }
}
