package edu.uiuc.groupmessage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

public class JuiceF2Worker extends Thread  {
  MemListNode currentNode;
  JuiceF2Worker(MemListNode current_node) {
    super();
    this.currentNode = current_node;
  }
  public void run() {
    try {
      // Merge all key files in SDFS
      ArrayList< String > fileList;
      fileList = currentNode.OprationSDFS("list","tfidf","");

      System.out.println(fileList);

      // Open JobLog and get the destination file name
      File DestFile = null;
      currentNode.OprationSDFS("get","JobLog","JobLog");
      BufferedReader s = null;
      try {
        s = new BufferedReader(new FileReader("JobLog"));
      } catch (FileNotFoundException ex){
        System.out.println(ex.getMessage());
      }

      String str = null;
      try {
        String exename = s.readLine();
        LinkedList<String> args = new LinkedList<String>();
        args.add(exename);
        int i = 1;
        while ((str = s.readLine())!= null){
          i++;
          if(i == 4)
            DestFile = new File(str);
        }
      } catch (IOException ex){
        System.out.println(ex.getMessage());
      }

      try{
        if(DestFile.exists())
          DestFile.delete();
        DestFile.createNewFile();
      }
      catch (Exception e) {
        e.printStackTrace();
      }

      for(int i=0;i<fileList.size();i++)
      {
        try {
          // create writer for file to append to
          BufferedWriter out = new BufferedWriter(new FileWriter(DestFile, true));
          currentNode.OprationSDFS("get",fileList.get(i),fileList.get(i));
          // create reader for file to append from
          BufferedReader in = new BufferedReader(new FileReader(fileList.get(i)));
          String Str;
          while ((Str = in.readLine()) != null) {
            out.write(Str);
            out.write("\n");
          }
          in.close();
          out.close();
          File key_file = new File(fileList.get(i));
          key_file.delete();
        } catch (IOException e) {
        }
      }

      currentNode.OprationSDFS("put",DestFile.getName(),DestFile.getName());
      DestFile.delete();
      currentNode.OprationSDFS("erase", "tfidf", "");
      // change phase to ready state
      currentNode.phase = 0;
      currentNode.createStateLogAndPut(currentNode.StateLog,currentNode.phase);
      // Delete the special juice file
      currentNode.OprationSDFS("delete",".JUICE_RUN",""); 

      // Juice Complete
      currentNode.getLOGGER().warning("Juice Execution Time: " + (System.currentTimeMillis() - currentNode.getJuiceTimestamp()));
      System.out.println("-------Juice is Completely Done-----------");
    } catch (InterruptedException ex) {
      ex.printStackTrace();
    }
  }
}
