package edu.uiuc.groupmessage;

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
             
             
             
             
             
