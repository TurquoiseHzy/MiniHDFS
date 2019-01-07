import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ClientProtocol extends Remote {
    ClientResponse create(String filename,String place, int blockCount) throws RemoteException;
    boolean delete(String name) throws RemoteException;
    boolean mkdir(String dirname) throws RemoteException;
    boolean rename(String oldName,String newName) throws RemoteException;
    ClientResponse download(String place) throws RemoteException;
    String pwd() throws RemoteException;
    String ls() throws RemoteException;
    boolean cd(String tar) throws RemoteException;
    void printTree() throws IOException;
}
