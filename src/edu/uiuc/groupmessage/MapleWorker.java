package edu.uiuc.groupmessage;

import java.util.LinkedList;
import java.io.IOException;
import java.io.FilenameFilter;
import java.io.File;

import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;
import edu.uiuc.groupmessage.GroupMessageProtos.Member;

class MapleWorker extends Thread {
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
    }
    /*
    public void cancel(){
        abort = true;
        // stop the tf thread first?????
        System.out.println("I am abort!!!!!!!!!!!!!!!!!!!!!!!!--------------------");
        this.stop();
    }*/
    
    public void run() {
        // get MapleExe
        System.out.println("I am in the Maple Worker");
        System.out.println("Prefix is "+prefix+", work is "+work);
        currentNode.getmserver().OprationSDFS("get","MapleExe","tf.class");  
        runMaple();
        
        // find the prefix_key_jobid(because ip might be same)
        // Also put them into SDFS
        FindFileAndPut(prefix);
        
        // Send back workdone message and ask for new work
        GroupMessage msg = GroupMessage.newBuilder()
        .setTarget(currentNode.getCurrentMember())
        .addArgstr(work)
        .addArgstr(jobid)
        .addArgstr(prefix)
        .setAction(GroupMessage.Action.MAPLE_WORK_DONE)
        .build();
        
        currentNode.sendMessageTo(msg,currentNode.getMemberList().get(0));
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
            System.out.println(file.getName() + " is in current folder");
            currentNode.getmserver().OprationSDFS("put",file.getName(),file.getName()+"_"+jobid);
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
            process.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch(IOException ex) {
            System.out.println("Unable to run mapleExe.\n");
        }
    }
}