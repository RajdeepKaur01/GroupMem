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

class Maplef2Master extends Thread {
    int phase;
    String prefix;
    MemListNode currentNode;
    Maplef2Master(MemListNode current_node,String prefix,int phase) {
        super();
        this.currentNode = current_node;
        this.prefix = prefix;
        this.phase = phase;
    }
    
    public void run() {
        currentNode.DistributeJob(prefix,phase);
    }
}
             
             
             
             
             
