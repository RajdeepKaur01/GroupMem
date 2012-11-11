package edu.uiuc.groupmessage;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import java.net.Socket;
import java.net.UnknownHostException;

import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;
import edu.uiuc.groupmessage.GroupMessageProtos.Member;
import com.google.protobuf.ByteString;

class SDFSClient extends Thread {
  private Member master;

  SDFSClient(String masterIP, int port){
    this.master = Member.newBuilder()
      .setPort(port)
      .setIp(masterIP)
      .build();
  }

  private ByteString readFile(String local_name) {
    File file = new File(local_name); 
    byte[] file_data = new byte[(int)file.length()];
    try {
      DataInputStream dis = new DataInputStream(new FileInputStream(file));
      dis.readFully(file_data);
      dis.close();
    } catch(Exception ex) {
      System.out.println(ex.getMessage());
      return null;
    }
    return ByteString.copyFrom(file_data);
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
  
  public void putFile(String local_name, String sdfs_name) {
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

    // send the message to the master and check the response
    rcv_msg = sendMessage(master, send_msg);
    if (rcv_msg.getAction() != GroupMessage.Action.FILE_LOCATION) {
      System.out.println("Received Unknown message action " + rcv_msg.getAction().name());
      return;
    }

    // Read file content from the file
    ByteString file_data = readFile(local_name);
    if (file_data == null) {
      return;
    }

    // Prepare for the PUT_FILE message
    Member target = rcv_msg.getTarget();
    System.out.println("Sending file to " + target.getIp() + "_" + target.getPort());
    send_msg = GroupMessage.newBuilder()
      .setTarget(target)
      .setAction(GroupMessage.Action.PUT_FILE)
      .setFileContent(file_data)
      .setFileName(sdfs_name)
      .build();


    // upload the file to the file location
    rcv_msg = sendMessage(rcv_msg.getTarget(), send_msg);

    // check the response
    switch (rcv_msg.getAction()) {
      case FILE_OK:
	System.out.println("Successfully sent the file " + local_name);
	break;
      case FILE_ERROR:
	System.out.println("Error sending the file " + local_name);
	break;
      case FILE_LOCATION:
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

	  // send the message to the master and check the response
	  rcv_msg = sendMessage(master, send_msg);
	  if (rcv_msg.getAction() != GroupMessage.Action.FILE_LOCATION) {
		  System.out.println("Received Unknown message action " + rcv_msg.getAction().name());
		  return;
	  }

	  // Prepare for the GET_FILE message
	  Member target = rcv_msg.getTarget();
	  System.out.println("Recieving file from" + target.getIp() + "_" + target.getPort());
	  send_msg = GroupMessage.newBuilder()
			  .setTarget(target)
			  .setAction(GroupMessage.Action.GET_FILE)
			  .setFileName(sdfs_name)
			  .build();


	  // read the file from the file location
	  rcv_msg = sendMessage(rcv_msg.getTarget(), send_msg);

	  // check the response
	  switch (rcv_msg.getAction()) {
	  case FILE_OK:
		  System.out.println("Successfully read the file " + local_name);
		  break;
	  case FILE_ERROR:
		  System.out.println("Error reading the file " + local_name);
		  break;
	  case FILE_NOT_EXIST:
		  System.out.println("File does not exist " + local_name);
		  break;
	  case FILE_LOCATION:
	  default:
		  System.out.println("Received Unknown action " + rcv_msg.getAction().name());
		  break;
	  }

	  // Write contents to local file
	  try {
		  File saved_file = new File(local_name);
		  FileOutputStream file_out = new FileOutputStream(saved_file);
		  file_out.write(rcv_msg.getFileContent().toByteArray());
		  file_out.flush();
		  file_out.close();
	  } catch (Exception ex) {
		  System.out.println(ex.getMessage());
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

  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Usage: java edu.uiuc.groupmessage.SDFSClient <master IP> <master port>");
      System.exit(-1);
    }
    SDFSClient client = new SDFSClient(args[0], Integer.parseInt(args[1]));
    BufferedReader std_in = new BufferedReader(new InputStreamReader(System.in));
    String str;
    System.out.println("Please enter command:");
    try {
      while (!(str = std_in.readLine()).equals("exit")) {
	String[] tokens = str.split(" ");
	if (tokens[0].equals("put") && tokens.length == 3) {
	  client.putFile(tokens[1], tokens[2]);
	} else if (tokens[0].equals("get") && tokens.length == 3) {
	  client.getFile(tokens[1], tokens[2]);
	} else if (tokens[0].equals("delete") && tokens.length == 2) {
	  client.deleteFile(tokens[1]);
	} else {
	  System.out.println("Invalid command!");
	  continue;
	}
	System.out.println("Please enter command:");
      }
    } catch (IOException ex) {
      System.out.println(ex.getMessage());
    }
  }
}
