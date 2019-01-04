
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;

// RMI客户端
public class DataNode extends BaseDataNode {


    public DataNode(int id){
        System.out.println("Creating datanode "+ id);
        this.dataNodeId = id;
        this.dir = "DataNode"+dataNodeId+"/";
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("intput id :");
        int id = scanner.nextInt();
        //int id = 2;
        DataNode dnApp = new DataNode(id);
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry("127.0.0.1",8089);
            if(dnApp.initialize(registry)){
                dnApp.start();
            }
        } catch (AccessException e) {
            System.out.println("access error");
        } catch (RemoteException e) {
            System.out.println(e);
        } catch (NotBoundException e) {
            System.out.println("not bound error");
        }
    }


    protected SocketWaitor socketWaiter;
    public class SocketWaitor extends Thread{
        public SocketWaitor(){
            super("SocketWaitor");
        }

        public void run(){
            try (ServerSocket serverSocket = new ServerSocket(port + dataNodeId)) {
                while (true) {
                    System.out.println("Listening");
                    Socket socket = serverSocket.accept();// 侦听并接受到此套接字的连接,返回一个Socket对象
                    SocketTransor socketThread = new SocketTransor(socket);
                    socketThread.start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    boolean initialize(Registry registry) throws RemoteException,NotBoundException{
        System.out.println("initialing....");
        this.Manager = (DataNodeProtocol) registry.lookup("RpcServer");
        if(this.Manager == null){
            return false;
        }
        heart = new Heart();
        reportor = new BlockReportor();
        socketWaiter = new SocketWaitor();
        File workDir=new File(dir);
        if(!workDir.exists()){//如果文件夹不存在
            workDir.mkdir();//创建文件夹
        }
        //socketWaiter.setPriority(Thread.MAX_PRIORITY);
        return true;
    }

    void start(){

        System.out.println("regist....");
        try {
            DataNodeResponse response = Manager.start(dataNodeId,url,port + dataNodeId);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("start....");
        heart.start();
        reportor.start();
        socketWaiter.start();
        return;
    }


}