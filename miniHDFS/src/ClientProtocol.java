import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ClientProtocol extends Remote {
    ClientResponse create(String filename,String place, int blockCount) throws RemoteException;
    boolean delete() throws RemoteException;
    boolean mkdir() throws RemoteException;
    boolean rename() throws RemoteException;
    //boolean download() throws RemoteException;
}
