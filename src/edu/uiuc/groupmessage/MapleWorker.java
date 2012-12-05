package edu.uiuc.groupmessage;

import java.util.LinkedList;
import java.io.IOException;
import java.io.FilenameFilter;
import java.io.File;
import java.io.RandomAccessFile;


import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;
import edu.uiuc.groupmessage.GroupMessageProtos.Member;

class MapleWorker extends Thread {
    String phase1doneprefix = "phase1_";
    boolean done;
    boolean abort;
    String prefix;
    String work;
    String jobid;
    MemListNode currentNode;
    MapleWorker(MemListNode current_node, String prefix, String work, String id) {
        super();
        this.currentNode = current_node;
        this.work = work;
        this.prefix = prefix;
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
            System.out.println("------Waiting to abort------------");
        }
    }
    
    public void run() {
        done = false;
        String mark_done = phase1doneprefix+"done_"+work;
        // first check if the job is aleady done by someone else before master failed
        LinkedList<String> returnlist = currentNode.OprationSDFS("list",mark_done,"");
        if (returnlist.size() == 0){// not done
              
            if (abort) {
                currentNode.sendAbortMessage();
                return;
            }
            // get MapleExe
            System.out.println("I am in the Maple Worker");
            System.out.println("Prefix is "+prefix+", work is "+work);
            currentNode.OprationSDFS("get","MapleExe","tf.class"); 
            if (abort) {
                currentNode.sendAbortMessage();
                return;
            }
            
            runMaple();
            if (abort) {
                currentNode.sendAbortMessage();
                return;
            }
            
            // find the prefix_key_jobid(because ip might be same)
            // Also put them into SDFS
            FindFileAndPut(prefix);
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
        .setAction(GroupMessage.Action.MAPLE_WORK_DONE)
        .build();
        
        currentNode.sendMessageTo(msg,currentNode.getMemberList().get(0));
        done = true;
    }
    
    public void FindFileAndPut(final String prefix)
	{
		String pathname = System.getProperty("user.dir");
        File folder = new File(pathname);
        File [] files = folder.listFiles(new FilenameFilter() {
            @Override
         	public boolean accept( File dir, String name ) {
                return name.startsWith(prefix);
            }
        } );
        for ( File file : files ){
            //System.out.println(file.getName() + " is in current folder");
            
            if (abort) {
                currentNode.sendAbortMessage();
                return;
            }
            
            currentNode.OprationSDFS("put",file.getName(),file.getName()+"_"+jobid);
            // remove from local, so that next time check file start with prefix, they are not uploaded again
            if(file.delete())
                System.out.println(file.getName() + " is deleted!!!!!!!!!!!!!");
            else
                System.out.println("Delete "+file.getName()+" is failed.!!!!!!!!!!!!!!!!!");
        }
	}
    
    
    public void runMaple(){
        System.out.println("Operation : java tf "+prefix+" "+work);
        Runtime runtime = Runtime.getRuntime();
        Process process = null;
        try {
            LinkedList< String > cmd_array = new LinkedList< String >();
            cmd_array.add("java");
            cmd_array.add("tf");
            cmd_array.add(prefix);
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
            System.out.println("Unable to run mapleExe.\n");
        }
    }
}