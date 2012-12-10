package edu.uiuc.groupmessage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

// distribute the initial jobs
class MapleMaster extends Thread {
  List< String > args;
  String prefix;
  MemListNode currentNode;
  int phase;
  MapleMaster(MemListNode current_node,List< String > args, int phase) {
    super();
    this.currentNode = current_node;
    this.args = args;
    prefix = args.get(1);
    this.phase = phase;
  }

  // phase 1, every one get a job, phase 2 every one find some files to merge
  public void run() {
    try {
      System.out.println("MapleMaster is up");

      // put exe into SDFS
      currentNode.OprationSDFS("put",args.get(0),"MapleExe");

      // put joblist into  SDFS
      String filename = "JobList";
      // remove last time "JobList"
      WriteJobList(filename);
      currentNode.OprationSDFS("delete","JobList_SDFS","");
      currentNode.OprationSDFS("put",filename,"JobList_SDFS");       

      // send job message and wait
      currentNode.DistributeJob(prefix,phase);
    } catch (InterruptedException ex) {
      ex.printStackTrace();
    }
  }

  public void WriteJobList(String filename)
  {
    try {
      BufferedWriter buf_writer = new BufferedWriter(new FileWriter(filename));
      for (int i = 2; i < args.size(); i++){
        String filen = args.get(1) + "_" + (new File(args.get(i))).getName() + "\n";
        buf_writer.write(filen);
      }
      buf_writer.close();
    } catch (IOException ex) {
      System.out.println("Unable to write JobList\n");
    }

  }
}
