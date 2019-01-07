import java.util.*;
public abstract class DataNodeManager extends DataNodeResponder {

    protected class DataNodeRuntime{
        public int dataNodeId;
        public long lassHeartbeatReceive;
        Map<String,ArrBlockInfo> arrBlockInfo;
        DataNodeResponse[] response;
        int responseNum = 0;
        public String url;
        public int port;

        DataNodeRuntime(int nId, String url, int port){
            this.dataNodeId = nId;
            this.lassHeartbeatReceive = getTime();
            this.arrBlockInfo = new HashMap<>();
            this.response = new DataNodeResponse[MACRO.MAX_RESPONSE_COUNT];
            this.url = url;
            this.port = port;
        }

        public void addResponse(DataNodeResponse r){
            this.response[responseNum] = r;
            responseNum ++;
        }
        public DataNodeResponse[] sendResponse(){
            DataNodeResponse[] r = new DataNodeResponse[responseNum];
            for(int i = 0 ; i < responseNum ; i ++){
                r[i] = response[i];
            }
            responseNum = 0;
            return r;
        }
    }

    Map<Integer, DataNodeRuntime> datanodeRuntime;
    Set<FileInfo> fileInfo;


    DataNodeManager(){
        this.datanodeRuntime = new HashMap<>();
        this.fileInfo = new HashSet<>();
    }

    void datanodeLost(DataNodeRuntime runtime){
        for(ArrBlockInfo b : runtime.arrBlockInfo.values()){
            b.block.arrangement.remove(b);
        }
    }

    DataNodeRuntime[] arrangeBlock(int blockCount, List<Integer> ignore) {
        DataNodeRuntime[] blockArrangement = new DataNodeRuntime[blockCount];
        /*list arrange*/
        int i = 0 ;
        Iterator<Map.Entry<Integer, DataNodeRuntime>> entries;
        while(i<blockCount) {
            entries = datanodeRuntime.entrySet().iterator();
            while (entries.hasNext() && i < blockCount) {
                Map.Entry<Integer, DataNodeRuntime> entry = entries.next();
                DataNodeRuntime runtime = entry.getValue();
                if(ignore.contains(runtime.dataNodeId)){
                    continue;
                }
                blockArrangement[i] = runtime;
                i++;
            }
        }
        return blockArrangement;
    }

    DataNodeRuntime[] arrangeBlock(int blockCount){
        return arrangeBlock(blockCount,new ArrayList<Integer>());
    }

    private void copyMsgWrite(DataNodeRuntime master,DataNodeRuntime[] copy,int count,BlockInfo block){
        for(int i = 0 ; i < count ; i ++){
            copy[i].addResponse(new CopyResponse(block.makeKey(),master.url,master.port));
            System.out.println("response node"+copy[i].dataNodeId+" copy " + block.makeKey() + " from "+master.dataNodeId);
            ArrBlockInfo arr = new ArrBlockInfo(block,copy[i].dataNodeId);
            block.arrangement.add(arr);
            copy[i].arrBlockInfo.put(block.makeKey(),arr);
        }
    }

    synchronized void heartbeatChecker(){
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

        boolean flag = true;
        System.out.println("checking copy...");
        if(datanodeRuntime.size() >= MACRO.COPY_FACTOR) {
            for (Iterator<FileInfo> it = fileInfo.iterator();it.hasNext();) {
                FileInfo f = it.next();
                flag = true;
                for (BlockInfo b : f.blocks) {
                    int copies = b.arrangement.size();
                    //System.out.println("block "+ b.makeKey() + " copy " + copies);
                    if(copies == 0){
                        flag = false;
                    }
                    if (copies < MACRO.COPY_FACTOR) {
                        System.out.println("Finding "+ b.makeKey()+" copies lost, try to copy");
                        DataNodeRuntime masterCopy = datanodeRuntime.get(b.arrangement.get(0).dataNodeId);
                        DataNodeRuntime[] toCopy = arrangeBlock(MACRO.COPY_FACTOR - copies, b.owner());
                        copyMsgWrite(masterCopy,toCopy,MACRO.COPY_FACTOR - copies,b);
                    }
                }
                if(!flag){
                    System.out.println("0copies");
                    ((FileTree.FileTreeNode)f.node).remove();
                    it.remove();
                }
            }
        }
    }


}
