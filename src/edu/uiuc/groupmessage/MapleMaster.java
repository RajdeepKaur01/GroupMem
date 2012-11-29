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
        OprationSDFS("put",args.get(0),"MapleExe");
      
        // put joblist into  SDFS
        String filename = "JobList";
        // remove last time "JobList"
        WriteJobList(filename);
        OprationSDFS("put",filename,"JobList_SDFS");       
        
        // send job message and wait
        DistributeJob(prefix,phase);
    }
    
    public LinkedList<String> OprationSDFS(String op,String str1, String str2)
    {
        System.out.println("Operation: "+op+" "+str1+" "+str2);
        LinkedList<String> returnlist = new LinkedList<String>();
        Runtime runtime = Runtime.getRuntime();
        Process process = null;
        BufferedWriter output = null;
        try {
            LinkedList< String > cmd_array = new LinkedList< String >();
            cmd_array.add("java");
            cmd_array.add("-cp");
            cmd_array.add("../GroupMem-mp3/bin:lib/protobuf-java-2.4.1.jar");
            cmd_array.add("edu.uiuc.groupmessage.SDFSClient");
            cmd_array.add(currentNode.getMemberList().get(0).getIp());
            cmd_array.add("6611");
            cmd_array.add(op);
            cmd_array.add(str1);
            if ((op.equals("put"))||(op.equals("get")))
                cmd_array.add(str2);
            process = runtime.exec(cmd_array.toArray(new String[cmd_array.size()]));
            process.waitFor();
            BufferedReader result = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
			while((line = result.readLine()) != null) {
				returnlist.add(line);
			}            
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch(IOException ex) {
            System.out.println("Unable to "+op+" "+str1+" "+str2+" in SDFS.\n");
        }
        //for ( int i = 0; i < returnlist.size(); i++)
        //    System.out.println("returnlist("+i+") = "+ returnlist.get(i)+", "+op);
        return returnlist;
    }
    
    public void DistributeJob(String prefix,int phase){
        
        LinkedList< Member > memberList = currentNode.getMemberList();
        for (int i = 0 ; i < memberList.size(); i++)
		{
            // find proper job
            job TopJob = findjob(phase);
            if (TopJob == null)
                return;
            
            Member member = memberList.get(i);
            GroupMessage send_msg = null;
            if (phase == 1){
                //System.out.println("Distribute Maple Job Message in Phase 1!!!!!!!!!!!!!");
                send_msg = GroupMessage.newBuilder()
                .setTarget(member)
                .addArgstr(prefix)// pass prefix
                .addArgstr(TopJob.getname()) // pass filename
                .addArgstr(Integer.toString(TopJob.getid()))// pass job id
                .setAction(GroupMessage.Action.MAPLE_WORK)
                .build();
            } else if (phase == 2){
                //System.out.println("Distribute Maple Job Message in Phase 2!!!!!!!!!!!!!");
                send_msg = GroupMessage.newBuilder()
                .setTarget(member)
                .addArgstr(prefix)// pass prefix
                .addArgstr(TopJob.getname()) // pass filename
                .addArgstr(Integer.toString(TopJob.getid()))// pass job id
                .setAction(GroupMessage.Action.MAPLE_F2_WORK)
                .build();
            } 
            
            GroupMessage rcv_msg = currentNode.sendMessageWaitResponse(member, send_msg);
            if (rcv_msg.getAction() == GroupMessage.Action.NODE_FREE){
                System.out.println("Member "+i+" ack.");
            } else {
                System.out.println("Member "+i+" does not ack. Should Not Happen!!!!");
            }
                
            // update job time
            TopJob = currentNode.getQ().remove();
            TopJob.settime(System.currentTimeMillis());
            currentNode.getQ().add(TopJob);
		}
    }
    
    public job findjob(int phase){
        job TopJob = currentNode.getQ().peek();
        while ((TopJob == null) || (currentNode.getJobDone().get(TopJob.getid()) == true)){
            if (TopJob == null){
                System.out.println("Queue is empty");
                
                GroupMessage msg = null;
                // send message to master to begin phase 2
                if (phase == 1){
                    msg = GroupMessage.newBuilder()
                    .setTarget(currentNode.getCurrentMember())
                    .addArgstr(prefix)
                    .setAction(GroupMessage.Action.MAPLE_PHASE_ONE_DONE)
                    .build();
                } else if (phase == 2){ 
                    msg = GroupMessage.newBuilder()
                    .setTarget(currentNode.getCurrentMember())
                    .addArgstr(prefix)
                    .setAction(GroupMessage.Action.MAPLE_PHASE_TWO_DONE)
                    .build();
                } 
                
                currentNode.sendMessageTo(msg,currentNode.getMemberList().get(0));
                
                return null;
            } else if (currentNode.getJobDone().get(TopJob.getid()) == true){
                System.out.println("Job "+TopJob.getid()+" is done. Throw it out.");
                currentNode.getQ().remove();
                TopJob = currentNode.getQ().peek();
            }
        }
        return TopJob;
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
