import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface DataNodeProtocol extends Remote{
    boolean start(int nodeId,String url,int port) throws RemoteException;
    void end(int nodeId) throws RemoteException;
    DataNodeResponder.DataNodeResponse[] heartbeat(int nodeId) throws RemoteException;
    DataNodeResponder.DataNodeResponse[] updateBlockInfo(int nodeId, List<DataNodeResponder.BlockInfo> blocks) throws RemoteException;
}
