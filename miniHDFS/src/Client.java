
import java.io.*;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Collections;
import java.util.Scanner;

// RMI客户端
public class Client {

    protected static final int BLOCK_LENGTH = 10;
    ClientProtocol Manager;
    private boolean create(String filename,String place){
        System.out.println("create");
        File f = null;
        FileInputStream fi = null;
        try {
            f = new File(filename);
            fi = new FileInputStream(f);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(f.length() + "divided to "+((int) f.length() / BLOCK_LENGTH + 1)+ " blocks");
        if(f == null || fi == null){
            return false;
        }
        try {
            ClientResponse response = Manager.create(filename, place, (int) f.length() / BLOCK_LENGTH + 1);
            for(ClientResponse.DataNodeCreateQuery q : response.queries) {
                System.out.println(q.BlockId+" "+q.url +":"+q.port);
            }

            Collections.sort(response.queries);
            for(ClientResponse.DataNodeCreateQuery q : response.queries){
                Socket s = q.createSocket();
                if(s == null){
                    return false;
                }
                OutputStream os = s.getOutputStream();;
                PrintWriter pw = new PrintWriter(os);
                pw.write(filename+"_"+q.BlockId+"\n");
                pw.flush();
                byte[] buf = new byte[BLOCK_LENGTH];
                fi.read(buf,0,BLOCK_LENGTH);
                os.write(buf);
                s.close();
                os.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    private void delete(){
        System.out.println("delete");

    }

    private void mkdir(){
        System.out.println("mkdir");

    }

    private void rename(){
        System.out.println("rename");

    }
    /*
    private void download(){
        System.out.println("download");

    }
*/


    public static void main(String[] args) {
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry("127.0.0.1",8089);
        } catch (RemoteException e) {

        }

        Client app = new Client();
        try {
            app.Manager = (ClientProtocol) registry.lookup("RpcServer");
        } catch (Exception e){
            e.printStackTrace();
        }
        Scanner sc = new Scanner(System.in);
        while(true){
            System.out.println("what do u want:");
            String s = sc.nextLine();
            String[] ss = s.trim().split(" ");
            if(ss[0].equals("create")){
                app.create(ss[1],ss[2]);
            }
            else if(ss[0].equals("delete")){
                app.delete();
            }
            else if(ss[0].equals("rename")) {
                app.rename();
            }
            else if(ss[0].equals("mkdir")){
                app.mkdir();
            }
        }


    }

}