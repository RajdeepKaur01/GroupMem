package edu.uiuc.groupmessage;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.Socket;

import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;
import edu.uiuc.groupmessage.GroupMessageProtos.Member;

class FileClient extends Thread {
  final int CHUCK_SIZE = 4096;
  byte[] buf;
  Member receiver;
  File file;
  String fileName;
  int action;
  GroupMessage result;
  FileClient(Member receiver, String file_name, File file, int action) {
    super();
    this.receiver = receiver;
    this.file = file;
    this.fileName = file_name;
    this.action = action;
    buf = new byte[CHUCK_SIZE];
  }

  public GroupMessage getResult() {
    return result;
  }

  public void run() {
    System.out.println("FileClient is up");
    switch (action) {
//      case GroupMessage.Action.GET_FILE_VALUE:
//	BufferedOutputStream file_out = new BufferedOutputStream(new FileOutputStream(file));
//	break;
      default:
	try {
	  BufferedInputStream file_in = new BufferedInputStream(new FileInputStream(file));
	  Socket sock = new Socket(receiver.getIp(), receiver.getPort() + 2); 
	  DataOutputStream sock_out = new DataOutputStream(sock.getOutputStream());
	  InputStream sock_in = sock.getInputStream();
	  sock_out.writeInt(action);
	  char[] chars = fileName.toCharArray();
	  sock_out.writeInt(chars.length);
	  sock_out.writeChars(fileName);
	  sock_out.writeLong(file.length());
	  int res = 0;
	  while ((res = file_in.read(buf, 0, buf.length)) != -1) {
	    sock_out.write(buf, 0, res);
	  }
	  sock_out.flush();

	  result = GroupMessage.parseDelimitedFrom(sock_in);
	  System.out.println(result.toString());

	  // Close I/O
	  sock_out.close();
	  file_in.close();
	  sock.close();
	} catch (Exception ex) {
	  System.out.println(ex.getMessage());
	}
	break;
    }
    System.out.println("FileClient is down");
  }
}
