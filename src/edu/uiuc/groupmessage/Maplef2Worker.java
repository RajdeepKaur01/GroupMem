package edu.uiuc.groupmessage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Scanner;

import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;
import edu.uiuc.groupmessage.GroupMessageProtos.Member;

class Maplef2Worker  extends Thread {
    String prefix;
    String work;
    String jobid;
    MemListNode currentNode;
    Maplef2Worker(MemListNode current_node, String prefix, String work, String id) {
        super();
        this.currentNode = current_node;
        this.prefix = prefix;
        this.work = work;        
        this.jobid = id;
    }
    
    public void run() {
        // get MapleExe
        System.out.println("I am in the Maple Worker. f2");
        System.out.println("work is "+work);
        
        LinkedList<String> workfiles = currentNode.getmserver().OprationSDFS("list",work,"");
        String filename = "done_"+work;
        try{
            RandomAccessFile raf = new RandomAccessFile(filename, "rws");
            for (int i = 0; i < workfiles.size(); i++){
                currentNode.getmserver().OprationSDFS("get",workfiles.get(i),workfiles.get(i));
                //System.out.println(workfiles.get(i));
                BufferedReader s = new BufferedReader(new FileReader(workfiles.get(i)));
                raf.seek(raf.length());
                String str = s.readLine();
                //System.out.println(str);
                raf.writeBytes(str);
                raf.writeBytes("\n");
                
                // then delete this file locally
                File file = new File(workfiles.get(i));
                if(file.delete())
                    System.out.println(file.getName() + " is deleted!");
                else
                    System.out.println("Delete "+file.getName()+" is failed.");
                
                // delete it from SDFS
                currentNode.getmserver().OprationSDFS("delete",workfiles.get(i),"");
            }
            raf.close();
        } catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        currentNode.getmserver().OprationSDFS("put",filename,filename);
        // remove it from local
        File file = new File(filename);
        if(file.delete())
            System.out.println(file.getName() + " is deleted!");
        else
            System.out.println("Delete "+file.getName()+" is failed.");

        
        // Send back workdone message and ask for new work
        GroupMessage msg = GroupMessage.newBuilder()
        .setTarget(currentNode.getCurrentMember())
        .addArgstr(filename)
        .addArgstr(jobid)
        .addArgstr(prefix)
        .setAction(GroupMessage.Action.MAPLE_WORK_DONE)
        .build();
        
        currentNode.sendMessageTo(msg,currentNode.getMemberList().get(0));
    }
}