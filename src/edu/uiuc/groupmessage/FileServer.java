package edu.uiuc.groupmessage;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;
import edu.uiuc.groupmessage.GroupMessageProtos.Member;

class FileServer extends Thread {
  ServerSocket serverSock;
  MemListNode currentNode;
  FileServer(MemListNode current_node) {
    super();
    this.currentNode = current_node;
  }
  class FileServerWorker extends Thread {
    final int CHUCK_SIZE = 4096;
    byte[] buf;
    MemListNode currentNode;
    Member currentMember;
    String fileName;
    Socket sock;
    int action;
    FileServerWorker(MemListNode current_node, Socket sock) {
      super();
      buf = new byte[CHUCK_SIZE];
      this.currentNode = current_node;
      this.currentMember = current_node.getCurrentMember();
      this.sock = sock;
    }
    
    public String getFileName() {
      return fileName;
    }

    public int getAction() {
      return action;
    }

    public void run() {
      System.out.println("FileServerWorker is up");
      try {
	// Open I/O
	DataInputStream sock_in = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
	OutputStream sock_out = sock.getOutputStream();

        // Read the message action
	action = sock_in.readInt();
	switch (action) {
	case GroupMessage.Action.GET_FILE_VALUE:
	  {
            // Parse the SDFS file name
	    int name_size = sock_in.readInt();
	    char[] raw_name = new char[name_size];
	    for (int i = 0; i < name_size; i++) {
	      raw_name[i] = sock_in.readChar();
	    }
	    fileName = String.valueOf(raw_name);

            // Check if the file exists
	    File saved_file = new File(currentNode.getDirPath() + "/" + fileName);
	    if (saved_file.exists()) {
              // File exists
              GroupMessage.newBuilder()
                .setTarget(currentMember)
                .setAction(GroupMessage.Action.FILE_OK)
                .build()
                .writeDelimitedTo(sock_out);
	    } else {
              // File does not exist
              GroupMessage.newBuilder()
                .setTarget(currentMember)
                .setAction(GroupMessage.Action.FILE_NOT_EXIST)
                .build()
                .writeDelimitedTo(sock_out);
	      break;
            }

            // Send the file content
	    FileInputStream file_in = new FileInputStream(saved_file);
	    int res = 0;
	    while ((res = file_in.read(buf, 0, buf.length)) != -1) {
	      sock_out.write(buf, 0, res);
	    }
	    file_in.close();

            // Pass to MemListNode to process the file
	    currentNode.handlePutFile(fileName);
	    break;
	  }
	default: // put file or push file
	  {
            // Parse the SDFS file name
	    int name_size = sock_in.readInt();
	    char[] raw_name = new char[name_size];
	    for (int i = 0; i < name_size; i++) {
	      raw_name[i] = sock_in.readChar();
	    }
	    fileName = String.valueOf(raw_name);

            // Parse the SDFS file size
	    long file_size = sock_in.readLong();

            // Get the file
	    try {
              File tmp_file = File.createTempFile(fileName + (int) System.currentTimeMillis(), ".temp");
	      File saved_file = new File(currentNode.getDirPath() + "/" + fileName);
	      FileOutputStream file_out = new FileOutputStream(tmp_file);
	      int res = 0;
	      while (file_size > 0) {
		res = sock_in.read(buf, 0, buf.length);
		if (res == -1) {
		  break;
		}
		file_out.write(buf, 0, res);
		file_size -= res;
	      }
	      file_out.flush();
	      file_out.close();
              tmp_file.renameTo(saved_file);

	      // Send back the acknowledgement of the file
	      GroupMessage.newBuilder()
		.setTarget(currentMember)
		.setAction(GroupMessage.Action.FILE_OK)
		.build()
		.writeDelimitedTo(sock_out);	
	    } catch (Exception ex) {
	      System.out.println(ex.getMessage());
	    }

            // Hand the file name back to the current node to process
            switch (action) {
              case GroupMessage.Action.PUT_FILE_VALUE:
                currentNode.handlePutFile(fileName);
                break;
              case GroupMessage.Action.PUSH_FILE_VALUE:
                currentNode.handlePushFile(fileName);
                break;
              default:
                System.exit(-1);
                break;
            }
	    break;
	  }
	}

	// Close I/O
	sock_out.close();
	sock_in.close();
	sock.close();    
      } catch(IOException ex) {
	System.out.println(ex.getMessage());
      }
      System.out.println("FileServerWorker is down");
    }
  }
  public void run() {
    System.out.println("FileServer is up");
    try {
      serverSock = new ServerSocket(currentNode.getCurrentMember().getPort() + 2);
      Socket sock = null;
      while ((sock = serverSock.accept()) != null) {		
	FileServerWorker worker = new FileServerWorker(currentNode, sock);
	worker.start();
      }
      serverSock.close();
    } catch(Exception ex) {
      System.out.println(ex.getMessage());
    }
    System.out.println("FileServer is down");
  }
}
