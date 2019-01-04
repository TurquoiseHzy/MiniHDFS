import java.io.Serializable;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class NameNode extends DataNodeManager implements Serializable{

    public class RMIManager extends UnicastRemoteObject implements ClientProtocol, DataNodeProtocol {
        public RMIManager() throws RemoteException{
            super();

        }
        @Override
        public ClientResponse create(String filename,String place, int blockCount) throws RemoteException{
            ClientResponse response = new ClientResponse();
            for(int i = 0 ; i < blockCount ; ) {
                Iterator<Map.Entry<Integer, DataNodeRuntime>> entries = datanodeRuntime.entrySet().iterator();
                while(entries.hasNext()) {
                    Map.Entry<Integer, DataNodeRuntime> entry = entries.next();
                    DataNodeRuntime runtime = entry.getValue();
                    ClientResponse.DataNodeCreateQuery q = new ClientResponse.DataNodeCreateQuery();
                    q.BlockId = i;
                    q.url = runtime.url;
                    q.port = runtime.port;
                    response.queries.add(q);
                    ArrBlockInfo binfo = new ArrBlockInfo();
                    binfo.dataNodeId = runtime.dataNodeId;
                    binfo.blockId = i;
                    binfo.filename = place;
                    binfo.copies = 1;
                    blockInfo.add(binfo);
                    i++;
                    if(i == blockCount)
                        break;
                }
            }
            return response;
        }
        @Override
        public boolean delete() throws RemoteException{
            return true;
        }
        @Override
        public boolean mkdir() throws RemoteException{
            return true;
        }
        @Override
        public boolean rename() throws RemoteException{
            return true;
        }


        @Override
        public DataNodeResponse start(int nodeId,String url,int port) {
            // while start empty concerned
            System.out.println("DataNode "+ nodeId + " join, WELCOME!");
            DataNodeRuntime runtime = new DataNodeRuntime(nodeId, url, port);
            if(datanodeRuntime.containsKey(nodeId)){
                return new DataNodeResponse(-1);
            }
            datanodeRuntime.put(nodeId,runtime);
            return runtime.sendResponse(); // update 0
        }

        @Override
        public void end(int nodeId) {
            datanodeLost(datanodeRuntime.get(nodeId));
        }

        @Override
        public DataNodeResponse heartbeat(int nodeId){
            DataNodeRuntime runtime = datanodeRuntime.get(nodeId);
           // System.out.println("Recieve heartbeat from " + nodeId + "  add= " +runtime);
            runtime.lassHeartbeatReceive = getTime();
            return runtime.sendResponse();
        }

        @Override
        public DataNodeResponse updateBlockInfo(int nodeId, List<BlockInfo> blocks) {
            DataNodeRuntime runtime = datanodeRuntime.get(nodeId);
            for(BlockInfo b : blocks){
                runtime.blockInfo.add(b);
                ArrBlockInfo b1 = (ArrBlockInfo) b;
                b1.dataNodeId = nodeId;
                blockInfo.add(b1);
            }
            return runtime.sendResponse();
        }
    }


    public class NameNodeThread extends Thread{
        NameNodeThread() {
            super("NameNode");
        }
        @Override
        public void run(){
            while(true) {
                heartbeatChecker();
                show();
                try {
                    sleep(1000 * 10);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public void show(){
            System.out.println("Active DataNode:");
            for(int id : datanodeRuntime.keySet()){
                System.out.print("node "+ id + "   ");
                System.out.println(datanodeRuntime.get(id).lassHeartbeatReceive);
            }
            for(ArrBlockInfo b : blockInfo){
                b.print();
            }
        }

    }
    public static void main(String args[]){
        NameNode nnApp = new NameNode();
        Registry registry = null;
        try {
            // 创建一个服务注册管理器
            registry = LocateRegistry.createRegistry(8089);

        } catch (RemoteException e) {
            e.printStackTrace();
        }
        try {
            RMIManager server = nnApp.new RMIManager();
            registry.rebind("RpcServer", server);
            System.out.println("bind success");

            NameNodeThread nnThread = nnApp.new NameNodeThread();
            nnThread.run();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

}
