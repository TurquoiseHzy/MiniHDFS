import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
public abstract class DataNodeResponder {
    public class DataNodeResponse implements Serializable{
        public int msgType = -1;
        DataNodeResponse(int type){
            this.msgType = type;
        }
    }

    class CopyResponse extends DataNodeResponse{
        CopyResponse(String b,String u,int p){
            super(1);
            blockName = b;
            url = u;
            port = p;
        }

        String blockName;
        String url;
        int port;
    }

    class DeleteResponse extends DataNodeResponse{
        DeleteResponse(String b){
            super(2);
            blockName = b;
        }
        String blockName;
    }


    protected class FileInfo implements Serializable{
        public FileTree.TreeNode node;
        public String filename;
        public List<BlockInfo> blocks = new ArrayList<>();
        public void print() {
            System.out.println("File printing: " + filename);
        }
    }

    public FileInfo INCOMPLETE_FILE(String filename){
        FileInfo f = new FileInfo();
        f.filename = filename;
        return f;
    }

    protected class BlockInfo implements Serializable{
        public int blockId;
        public FileInfo file;
        public List<ArrBlockInfo> arrangement;

        BlockInfo(FileInfo f){
            arrangement = new ArrayList<>();
            file = f;
        }

        BlockInfo(){

        }

        List<Integer> owner(){
            List<Integer> l = new ArrayList<>();
            for(ArrBlockInfo a : arrangement){
                l.add(a.dataNodeId);
            }
            return l;
        }
        /*
        public String md5() throws NoSuchAlgorithmException {
            MessageDigest md = MessageDigest.getInstance("MD5");

            byte[] buffer = new byte[1024];
            int length = -1;
            while ((length = fis.read(buffer, 0, 1024)) != -1) {
                md.update(buffer, 0, length);
            }
            fis.close();

        }*/
        public void print(){
            System.out.println("file: "+file.filename+ "."+blockId);
        }

        public String makeKey(){
            return file.filename + ":" +blockId;
        }
    }

    protected class ArrBlockInfo implements Serializable{
        public BlockInfo block;
        public int dataNodeId;
        public String stat;
        ArrBlockInfo(BlockInfo b, int id){
           this.dataNodeId = id;
           this.block = b;
           this.stat = "new";
        }
        public void print(){
            block.print();
            System.out.println("save in "+ dataNodeId);
            System.out.println("stat:" + stat);
        }

        public void valid(BlockInfo b){
            if(this.stat.equals("new")) {
                this.stat = "valid";
            }
        }
    }

    public long getTime(){
        return System.currentTimeMillis();
    }
}


/*









 */