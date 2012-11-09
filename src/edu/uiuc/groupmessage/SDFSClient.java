package edu.uiuc.groupmessage;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
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

  public void putFile(String local_name, String sdfs_name) {
    if (local_name == null || sdfs_name == null) {
      return;
    }

    // Read file content from the file
    ByteString file_data = readFile(local_name);
    if (file_data == null) {
      return;
    }

    // send the message to the master
    sendMessage(GroupMessage.Action.PUT_FILE, sdfs_name, file_data);
  }

  private void sendMessage(GroupMessage.Action action,
			   String file_name,
			   ByteString file_content) {
    new Thread() {
      private Member master;
      private GroupMessage.Action action;
      private String fileName;
      private ByteString fileContent;
      public Thread init(Member master,
			 GroupMessage.Action action,
			 String file_name,
			 ByteString file_content) {
	this.master = master;
	this.action = action;
	this.fileName = file_name;
	this.fileContent = file_content;
	return this;
      }
      public void run() {
	try {
	  Socket sock = new Socket(master.getIp(), master.getPort());
	  InputStream sock_in = sock.getInputStream();
	  OutputStream sock_out = sock.getOutputStream();
	  GroupMessage.newBuilder()
	    .setTarget(master)
	    .setAction(action)
	    .setFileContent(fileContent)
	    .setFileName(fileName)
	    .build()
	    .writeDelimitedTo(sock_out);
	  sock_out.flush();
	  GroupMessage msg = GroupMessage.parseDelimitedFrom(sock_in);
	  switch (msg.getAction()) {
	    case FILE_OK:
	      System.out.println("Successfully sent the file " + fileName);
	      break;
	    case FILE_ERROR:
	      System.out.println("Error sending the file " + fileName);
	      break;
	    default:
	      System.out.println("Received Unknown action " + msg.getAction().name());
	      break;
	  }
	  sock_out.close();
	  sock_in.close();
	  sock.close();
	} catch (UnknownHostException ex) {
	  System.out.println(ex.getMessage());
	} catch (IOException ex) {
	  System.out.println(ex.getMessage());
	}
      }
    }.init(master, action, file_name, file_content).start();
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
	} else if (tokens[0].equals("delete") && tokens.length == 2) {
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
