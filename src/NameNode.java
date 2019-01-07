import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class NameNode extends DataNodeManager implements Serializable{


    private FileTree fileTree;
    NameNode(){
        fileTree = new FileTree();
    }

    NameNode(String fstmigo){
        File f = new File(fstmigo);
        if(!f.exists() ||(fileTree = FileTree.readXML(f))==null){
            fileTree = new FileTree();
        }
    }
    public class RMIManager extends UnicastRemoteObject implements ClientProtocol, DataNodeProtocol {
        public RMIManager() throws RemoteException{
            super();
        }
        @Override
        public String pwd() throws RemoteException{
            return fileTree.workDir.pwd();
        }
        @Override
        public String ls() throws RemoteException{
            return fileTree.workDir.ls();
        }
        @Override
        public boolean cd(String tar) throws RemoteException{
            return fileTree.cd(tar);
        }
        @Override
        public void printTree() throws IOException {
            File f = new File("tree.xml");
            if(!f.exists()){
                f.createNewFile();
            }
            fileTree.writeXML(f);
        }

        @Override
        public ClientResponse download(String place) throws  RemoteException{
            FileTree.TreeNode node;
            if((node = fileTree.get(place))==null || node.getClass() != FileTree.FileTreeNode.class){
                return null;
            }
            FileInfo fInfo = ((FileTree.FileTreeNode)node).info;
            ClientResponse response = new ClientResponse();
            for(BlockInfo b : fInfo.blocks){
                if(b.arrangement.size() < 1){
                    return  null;
                }
                else{
                    DataNodeRuntime runtime = datanodeRuntime.get(b.arrangement.get(0).dataNodeId);
                    ClientResponse.DataNodeCreateQuery q = new ClientResponse.DataNodeCreateQuery();
                    q.filename = fInfo.filename;
                    q.BlockId = b.blockId;
                    q.port = runtime.port;
                    q.url = runtime.url;
                    response.queries.add(q);
                }
            }
            return response;
        }

        @Override
        public ClientResponse create(String filename, String place, int blockCount) throws RemoteException{
            FileTree.FileTreeNode node = (FileTree.FileTreeNode)fileTree.insertFile(place);
            ClientResponse response = new ClientResponse();
            FileInfo fInfo =  new FileInfo();
            fInfo.node = node;
            node.info = fInfo;
            fInfo.filename = filename;
            fileInfo.add(fInfo);
            DataNodeRuntime[] dataNodeToAdd = arrangeBlock(blockCount);
            for(int i = 0 ; i < blockCount; i ++){
                BlockInfo bInfo = new BlockInfo(fInfo);
                ArrBlockInfo arrBInfo =  new ArrBlockInfo(bInfo,dataNodeToAdd[i].dataNodeId);
                dataNodeToAdd[i].addResponse(new DataNodeResponse(0));
                bInfo.blockId = i;
                fInfo.blocks.add(bInfo);
                bInfo.arrangement.add(arrBInfo);
                dataNodeToAdd[i].arrBlockInfo.put(bInfo.makeKey(),arrBInfo);
                ClientResponse.DataNodeCreateQuery q = new ClientResponse.DataNodeCreateQuery();
                q.filename = fInfo.filename;
                q.BlockId = i;
                q.url = dataNodeToAdd[i].url;
                q.port = dataNodeToAdd[i].port;
                response.queries.add(q);
            }
            return response;
        }
        @Override
        public boolean delete(String name) throws RemoteException{
            FileTree.TreeNode node = fileTree.get(name);
            if(node.getClass() != FileTree.FileTreeNode.class){
                return false;
            }
            FileInfo info = ((FileTree.FileTreeNode)node).info;
            for(BlockInfo b : info.blocks){
                for(ArrBlockInfo arr : b.arrangement){
                    arr.stat = "delete";
                }
            }
            fileInfo.remove(info);
            ((FileTree.FileTreeNode)node).remove();
            return true;
        }
        @Override
        public boolean mkdir(String dirname) throws RemoteException{
            return (fileTree.insertFile(dirname) != null);
        }
        @Override
        public boolean rename(String oldName,String newName) throws RemoteException{
            return fileTree.rename(oldName,newName);
        }


        @Override
        public boolean start(int nodeId,String url,int port) {
            if(datanodeRuntime.containsKey(nodeId)){
                return false;
            }
            System.out.println("DataNode "+ nodeId + " join, WELCOME!");
            DataNodeRuntime runtime = new DataNodeRuntime(nodeId, url, port);
            datanodeRuntime.put(nodeId,runtime);
            return true;
        }

        @Override
        public void end(int nodeId) {
            datanodeLost(datanodeRuntime.get(nodeId));
        }

        @Override
        public DataNodeResponse[] heartbeat(int nodeId){
            DataNodeRuntime runtime = datanodeRuntime.get(nodeId);
            runtime.lassHeartbeatReceive = getTime();
            return runtime.sendResponse();
        }

        @Override
        public DataNodeResponse[] updateBlockInfo(int nodeId, List<BlockInfo> blocks) {
            DataNodeRuntime runtime = datanodeRuntime.get(nodeId);
            ArrBlockInfo arrBInfo;
            for(BlockInfo b : blocks){
                System.out.println(runtime + " "+ runtime.arrBlockInfo + " "+ b);
                if((arrBInfo = runtime.arrBlockInfo.get(b.makeKey()) )!= null){
                    arrBInfo.valid(b);
                }
                else{
                    System.out.println("error: missing block from namenode");
                }
            }
            Iterator<Map.Entry<String, ArrBlockInfo>> entries = runtime.arrBlockInfo.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<String, ArrBlockInfo> entry = entries.next();
                ArrBlockInfo arr = entry.getValue();
                if( !arr.stat.equals("valid") ){
                    System.out.println("error: missing block from datanode");
                }
                else if( arr.stat.equals("delete")){
                    runtime.addResponse(new DeleteResponse(arr.block.makeKey()));
                }
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
                    sleep(1000 * MACRO.UPDATE_SCS);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public void show(){
            /*
            System.out.println("Active DataNode:");
            for(int id : datanodeRuntime.keySet()){
                System.out.print("node "+ id + "   ");
                System.out.println(datanodeRuntime.get(id).lassHeartbeatReceive);
            }
            for(FileInfo f : fileInfo){
                f.print();
            }*/
        }

    }
    public static void main(String args[]) throws UnknownHostException {
        NameNode nnApp = new NameNode();
        Registry registry = null;
        int port = 8088;
        String host ="";
        host = InetAddress.getLocalHost().getHostAddress();
        if(args.length == 1 && Integer.parseInt(args[0]) > 8000 && Integer.parseInt(args[0]) < 10000){
            port = Integer.parseInt(args[0]);
        }
        try {
            // 创建一个服务注册管理器
            registry = LocateRegistry.createRegistry(port);

        } catch (RemoteException e) {
            e.printStackTrace();
        }
        try {
            RMIManager server = nnApp.new RMIManager();
            System.setProperty("java.rmi.server.hostname","114.243.29.81");
            registry.rebind("RpcServer", server);
            System.out.println("bind success");

            NameNodeThread nnThread = nnApp.new NameNodeThread();
            nnThread.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
