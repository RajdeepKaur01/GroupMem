package edu.uiuc.groupmessage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;

class Maplef2Worker  extends Thread {
    String phase2doneprefix = "phase2_";
    boolean done;
    boolean abort;
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
        abort = false;
        done = false;
    }
    public void abort(){
        abort = true;
        System.out.println("-------I am here to abort------------");
        if (done == true){
            System.out.println("------Already finish thread------------");
            currentNode.sendAbortMessage();
        } else {
            System.out.println("------Waiting to abort f2------------");
        }
    }
    
    public void run() {
        done = false;
        if (abort) {
            currentNode.sendAbortMessage();
            return;
        }
        // check if the work is done before master fail
        String filename = "done_"+work;
        // first check if the job is aleady done by someone else before master failed
        LinkedList<String> returnlist = currentNode.OprationSDFS("list",filename,"");
        if (returnlist.size() == 0){    // not done
        
            System.out.println("I am in the Maple Phase 2 Worker.");
            System.out.println("work is "+work);
        
            if (abort) {
                currentNode.sendAbortMessage();
                return;
            }
            
            LinkedList<String> workfiles = currentNode.OprationSDFS("list",work,"");
            try{
                RandomAccessFile raf = new RandomAccessFile(work, "rws");
                for (int i = 0; i < workfiles.size(); i++){
                    
                    if (abort) {
                        currentNode.sendAbortMessage();
                        return;
                    }
                    
                    currentNode.OprationSDFS("get",workfiles.get(i),workfiles.get(i));
                    BufferedReader s = new BufferedReader(new FileReader(workfiles.get(i)));
                    raf.seek(raf.length());
                    String str = s.readLine();
                    raf.writeBytes(str);
                    raf.writeBytes("\n");
                
                    // then delete this file locally
                    currentNode.deletefile(workfiles.get(i));
                    
                    // Don't delete the phase 1 intermediate files 
                    // it might be done by other worker
                    s.close();
                }
                raf.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (abort) {
                currentNode.sendAbortMessage();
                return;
            }
            currentNode.OprationSDFS("put",work,phase2doneprefix+work);
            // construct done file
            try{
                RandomAccessFile raf = new RandomAccessFile(filename, "rws");
                raf.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            currentNode.OprationSDFS("put",filename,filename);
            
            // remove it from local
            currentNode.deletefile(filename);
            currentNode.deletefile(work);
            done = true;
        }
        
        // Send back workdone message and ask for new work
        GroupMessage msg = GroupMessage.newBuilder()
        .setTarget(currentNode.getCurrentMember())
        .addArgstr(filename)
        .addArgstr(jobid)
        .addArgstr(prefix)
        .setAction(GroupMessage.Action.MAPLE_PHASE_TWO_SINGLE_WORK_DONE)
        .build();
        
        currentNode.sendMessageTo(msg,currentNode.getMemberList().get(0));
        done = true;
    }
}