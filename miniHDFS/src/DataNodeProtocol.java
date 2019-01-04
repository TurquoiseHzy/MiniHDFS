import java.rmi.Remote;
import java.util.List;
import java.rmi.RemoteException;
public interface DataNodeProtocol extends Remote{
    DataNodeResponsor.DataNodeResponse start(int nodeId,String url,int port) throws RemoteException;
    void end(int nodeId) throws RemoteException;
    DataNodeResponsor.DataNodeResponse heartbeat(int nodeId) throws RemoteException;
    DataNodeResponsor.DataNodeResponse updateBlockInfo(int nodeId, List<DataNodeResponsor.BlockInfo> blocks) throws RemoteException;
}
