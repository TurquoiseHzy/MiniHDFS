import java.util.*;
public class DataNodeManager extends DataNodeResponsor {
    private int COPY_FACTOR = 2;

    protected class DataNodeRuntime implements Comparable<DataNodeRuntime>{
        public int dataNodeId;
        public long lassHeartbeatReceive;
        public Set<BlockInfo> blockInfo;
        public DataNodeResponse response;
        public String url;
        public int port;

        public DataNodeRuntime(int nId,String url,int port){
            this.dataNodeId = nId;
            this.lassHeartbeatReceive = getTime();
            this.blockInfo = new HashSet<BlockInfo>();
            this.response = new DataNodeResponse(0);
            this.url = url;
            this.port = port;
        }

        public DataNodeResponse sendResponse(){
            DataNodeResponse r = this.response;
            this.response = null;
            return r;
        }

        @Override
        public int compareTo(DataNodeRuntime o) {
            if(this.blockInfo.size() < o.blockInfo.size()) {
                return 1;
            }
            else if(this.blockInfo.size() == o.blockInfo.size()) {
                return 0;
            }
            else return -1;
        }



    }

    protected Map<Integer, DataNodeRuntime> datanodeRuntime;
    protected Set<FileInfo> fileInfo;
    protected Set<ArrBlockInfo> blockInfo;

    protected DataNodeManager(){
        this.datanodeRuntime = new HashMap<>();
        this.fileInfo = new HashSet<>();
        this.blockInfo = new HashSet<>();
    }

    protected void datanodeLost(DataNodeRuntime runtime){

        //check block of copy_factor
        for(BlockInfo b : runtime.blockInfo){
            b.copies = b.copies - 1;
        }


    }

    protected void heartbeatChecker(){

        //check all heartbeat
        Iterator<Map.Entry<Integer, DataNodeRuntime>> entries = datanodeRuntime.entrySet().iterator();
        while(entries.hasNext()) {
            Map.Entry<Integer, DataNodeRuntime> entry = entries.next();
            DataNodeRuntime runtime = entry.getValue();
            long clock = getTime();
            if(clock - runtime.lassHeartbeatReceive > 600){
                System.out.println("Error: DATANODE " + runtime.dataNodeId + " lost.");
                datanodeLost(runtime);
                entries.remove();
            }
        }

        Iterator<ArrBlockInfo> it = blockInfo.iterator();
        while(it.hasNext()){
            ArrBlockInfo info = it.next();
            System.out.println(info.filename+"_"+info.blockId+"in"+info.dataNodeId);
        }
    }


    protected void blockSend2datanode(BlockInfo block,int datanodeId){
        DataNodeRuntime runtime = datanodeRuntime.get(datanodeId);
        runtime.blockInfo.add(block);
    }


}
