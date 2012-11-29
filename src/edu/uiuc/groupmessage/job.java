package edu.uiuc.groupmessage;

class job implements Comparable<job> 
{
    String name;
    long time;
    int id;
    job(String name, long time, int id){
        this.name = name;
        this.time = time;
        this.id = id;
    }
    long gettime(){
        return this.time;
    }
    void settime(long time){
        this.time = time;
    }
    String getname(){
        return this.name;
    } 
    int getid(){
        return this.id;
    }        
    @Override
    public int compareTo(job arg0) {
        if (this.gettime() > arg0.gettime())
            return 1;
        else
            return -1;
    }
}