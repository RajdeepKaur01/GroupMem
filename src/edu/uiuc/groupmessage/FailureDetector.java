package edu.uiuc.groupmessage;

import java.util.TimerTask;

class FailureDetector extends TimerTask {
  private MemListNode currentNode;
  FailureDetector(MemListNode current_node) {
    super();
    this.currentNode = current_node;
  }
  public void run() {
    currentNode.detectFailure();
  }
}
