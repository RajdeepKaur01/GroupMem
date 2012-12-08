package edu.uiuc.groupmessage;

import java.util.LinkedList;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.io.File;

import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;

class JuiceWorker extends Thread {
    String phase1doneprefix = "tfidf_";
    boolean done;
    boolean abort;
    String prefix;
    String work;
    String jobid;
    MemListNode currentNode;
    JuiceWorker(MemListNode current_node, String prefix, String work, String id) {
        super();
        this.currentNode = current_node;
        this.work = work;
        this.prefix = prefix;
        this.jobid = id;
        done = false;
        abort = false;
    }
    
    public void abort(){
        abort = true;
        System.out.println("-------I am here to abort------------");
        if (done == true){
            System.out.println("------Already finish thread------------");
            currentNode.sendAbortMessage();
        } else {
            System.out.println("------Waiting to abort------------");
        }
    }
    
    public void run() {
        done = false;
        String mark_done = phase1doneprefix+ work;
        // first check if the job is aleady done by someone else before master failed
        LinkedList<String> returnlist = currentNode.OprationSDFS("list",mark_done,"");
        if (returnlist.size() == 0){// not done
            
            if (abort) {
                currentNode.sendAbortMessage();
                return;
            }
            // get JuiceExe
            System.out.println("I am in the Juice Worker");
            System.out.println("Prefix is "+prefix+", work is "+work);
            currentNode.OprationSDFS("get","JuiceExe","tf_idf.class"); 
            if (abort) {
            	currentNode.sendAbortMessage();
            	return;
            }
            runJuice();
            if (abort) {
            	currentNode.sendAbortMessage();
            	return;
            }
            // put job done file into SDFS
            try {
                RandomAccessFile raf = new RandomAccessFile(mark_done, "rws");
                raf.close();
            } catch (IOException ex) {
                System.out.println("Unable to write JobList\n");
            }
            currentNode.OprationSDFS("put",mark_done,mark_done);
            currentNode.deletefile(mark_done);
        } else
            System.out.println(mark_done+" is already exist!");
        
        if (abort) {
        	currentNode.sendAbortMessage();
        	return;
        }
        // Send back workdone message and ask for new work
        GroupMessage msg = GroupMessage.newBuilder()
        .setTarget(currentNode.getCurrentMember())
        .addArgstr(work)
        .addArgstr(jobid)
        .addArgstr(prefix)
        .setAction(GroupMessage.Action.JUICE_WORK_DONE)
        .build();
        
        currentNode.sendMessageTo(msg,currentNode.getMemberList().get(0));
        done = true;
    }
    
    
    public void runJuice(){
        System.out.println("Operation :java tf_idf "+ work);
        Runtime runtime = Runtime.getRuntime();
        Process process = null;
        currentNode.OprationSDFS("get",work,work);  
        try {
            LinkedList< String > cmd_array = new LinkedList< String >();
            cmd_array.add("java");
            cmd_array.add("tf_idf");
            cmd_array.add(work);
            process = runtime.exec(cmd_array.toArray(new String[cmd_array.size()]));
            while (abort == false){
                try{
                    process.waitFor();
                    break;
                } catch (InterruptedException e) {
                    if (abort == true){
                        // stop the process and return
                        process.destroy();
                        System.out.println("Process is destroyed.------");
                    }
                }
            }
        } catch(IOException ex) {
            System.out.println("Unable to run juiceExe.\n");
        }
        currentNode.OprationSDFS("put","tfidf_" + work, "tfidf_" + work);
        File tf_file = new File(work);
        tf_file.delete();      
    }
}
