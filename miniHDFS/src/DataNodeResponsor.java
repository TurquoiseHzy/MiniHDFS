import java.io.Serializable;
import java.util.*;
public class DataNodeResponsor {
    protected class DataNodeResponse implements Serializable{
        public int msgType = -1;
        public DataNodeResponse(){

        }

        public DataNodeResponse(int type){
            this.msgType = type;
        }
    }

    protected class FileInfo implements Serializable{
        public String filename;
        public int blockNum;
        public List<BlockInfo> blocks;
    }


    protected class BlockInfo implements Serializable{
        public String filename;
        public int blockId;
        public int copies;

        public void print(){
            System.out.println("file: "+filename+ "."+blockId+" copy: "+copies);
        }
    }

    protected class ArrBlockInfo extends BlockInfo{
        public int dataNodeId;

        public void print(){
            super.print();
            System.out.println("save in "+ dataNodeId);
        }
    }

    public long getTime(){
        return System.currentTimeMillis();
    }
}


/*









 */