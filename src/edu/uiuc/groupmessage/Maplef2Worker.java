package edu.uiuc.groupmessage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import edu.uiuc.groupmessage.GroupMessageProtos.GroupMessage;

class Maplef2Worker  extends Thread {
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
  public void abort() {
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
    try {
      done = false;
      if (abort) {
        currentNode.sendAbortMessage();
        return;
      }
      // check if the work is done before master fail
      String filename = "done_" + work;
      // first check if the job is aleady done by someone else before master failed
      ArrayList<String> returnlist = currentNode.OprationSDFS("list", filename, "");
      if (returnlist.size() == 0) {    // not done

        System.out.println("I am in the Maple Phase 2 Worker.");
        System.out.println("work is " + work);

        if (abort) {
          currentNode.sendAbortMessage();
          return;
        }

        ArrayList<String> workfiles = currentNode.OprationSDFS("list", work, "");
        try{
          BufferedWriter buf_writer = new BufferedWriter(new FileWriter(work));
          for (int i = 0; i < workfiles.size(); i++) {
            if (abort) {
              currentNode.sendAbortMessage();
              buf_writer.close();
              return;
            }

            currentNode.OprationSDFS("get", workfiles.get(i), workfiles.get(i));
            File file = new File(workfiles.get(i));
            if (!file.exists()) {
              System.out.println(workfiles.get(i) + " does not exist");
              continue;
            }
            BufferedReader s = new BufferedReader(new FileReader(workfiles.get(i)));
            String str;
            while((str = s.readLine()) != null) {
              buf_writer.write(str);
              buf_writer.newLine();
            }

            // then delete this file locally
            currentNode.deletefile(workfiles.get(i));

            // Don't delete the phase 1 intermediate files 
            // it might be done by other worker
            s.close();
          }
          buf_writer.close();
//        } catch (NullPointerException ex) {
//          throw new InterruptedException();
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }
        if (abort) {
          currentNode.sendAbortMessage();
          return;
        }
        currentNode.OprationSDFS("put", work, work);
        // construct done file
        try{
          File file = new File(filename);
          file.createNewFile();
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
    } catch (InterruptedException ex) {
      System.out.println("Maplef2Worker is aborted");
      currentNode.sendAbortMessage();
    }
  }
}
