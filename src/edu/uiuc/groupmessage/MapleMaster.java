package edu.uiuc.groupmessage;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.io.RandomAccessFile;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;

import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;
import edu.uiuc.groupmessage.GroupMessageProtos.Member;

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
        System.out.println("MapleMaster is up");
        
        // put exe into SDFS
        currentNode.OprationSDFS("put",args.get(0),"MapleExe");
      
        // put joblist into  SDFS
        String filename = "JobList";
        // remove last time "JobList"
        WriteJobList(filename);
        currentNode.OprationSDFS("put",filename,"JobList_SDFS");       
        
        // send job message and wait
        currentNode.DistributeJob(prefix,phase);
    }
    
    public void WriteJobList(String filename)
    {
        try {
            RandomAccessFile raf = new RandomAccessFile(filename, "rws");
            
            for (int i = 2; i < args.size(); i++){
                //System.out.println("args("+i+") = "+args.get(i));
                String filen = args.get(1)+"_"+args.get(i)+"\n";
                raf.writeBytes(filen);
            }
            raf.close();
        } catch (IOException ex) {
            System.out.println("Unable to write JobList\n");
        }
        
    }
}
