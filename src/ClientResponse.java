import java.io.Serializable;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class ClientResponse implements Serializable{
    public static class DataNodeCreateQuery implements Comparable<DataNodeCreateQuery>,Serializable{
        public String url;
        public int port;
        public int BlockId;
        public String filename;

        Socket createSocket(){
            try {
                return new Socket(url, port);
            }catch (Exception e){
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public int compareTo(DataNodeCreateQuery o) {
            if(this.BlockId < o.BlockId){
                return -1;
            }
            else if (this.BlockId > o.BlockId){
                return 1;
            }
            return 0;
        }
    }
    List<DataNodeCreateQuery> queries = new ArrayList<>();

}
