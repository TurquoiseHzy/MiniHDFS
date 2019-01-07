import java.io.*;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Collections;
import java.util.Scanner;

// RMI客户端
public class Client {

    private ClientProtocol Manager;
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
        if(f == null || fi == null){
            return false;
        }
        try {
            int count = (int) f.length() / MACRO.BLOCK_LENGTH;
            if(f.length() % MACRO.BLOCK_LENGTH != 0){
                count ++;
            }

            String[] filedir = filename.trim().split("/");
            String[] placedir = place.trim().split("/");
            place = place.trim();
            if(place.endsWith("/")){
                place += filedir[filedir.length - 1];
            }
            ClientResponse response = Manager.create(filedir[filedir.length - 1] +"_" +placedir[placedir.length - 1], place, count);
            for(ClientResponse.DataNodeCreateQuery q : response.queries) {
                System.out.println(q.BlockId+" "+q.url +":"+q.port);
            }
            Collections.sort(response.queries);
            BufferedReader bf = new BufferedReader(new InputStreamReader(fi));
            for(ClientResponse.DataNodeCreateQuery q : response.queries){
                Socket s = q.createSocket();
                if(s == null){
                    return false;
                }
                PrintWriter pw = new PrintWriter(s.getOutputStream());
                pw.write("update\n"+q.filename+":"+q.BlockId+"\n");
                System.out.println("update\n"+q.filename+":"+q.BlockId+"\n");
                pw.flush();
                char[] buf = new char[MACRO.BLOCK_LENGTH];
                bf.read(buf,0,MACRO.BLOCK_LENGTH);
                pw.write(buf,0,MACRO.BLOCK_LENGTH);
                pw.flush();
                s.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    private void download(String place,String name) throws IOException {
        ClientResponse response = Manager.download(place);
        if (response == null){
            System.out.println("not exist");
        }
        else{
            PrintWriter printWriter = new PrintWriter(new FileOutputStream(name));
            Collections.sort(response.queries);
            for(ClientResponse.DataNodeCreateQuery q : response.queries){
                Socket s = q.createSocket();
                if(s == null){
                    System.out.println("connection failed");
                    return;
                }
                PrintWriter pw = new PrintWriter(s.getOutputStream());
                pw.write("download\n"+q.filename+":"+q.BlockId+"\n");
                System.out.println("download\n"+q.filename+":"+q.BlockId+"\n");
                pw.flush();
                InputStream inputStream = s.getInputStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                String info = bufferedReader.readLine();
                System.out.println("read:"+ info );
                if(info.equals("OK")) {
                    char[] buf = new char[MACRO.BLOCK_LENGTH];
                    bufferedReader.read(buf, 0, MACRO.BLOCK_LENGTH);
                    printWriter.write(buf,0,MACRO.BLOCK_LENGTH);
                    printWriter.flush();
                    bufferedReader.close();
                    inputStream.close();
                }
                else{
                    System.out.println("download from None:"+info);
                }
                s.close();
            }
        }
    }

    private void delete(String deletename) throws RemoteException {
        if(Manager.delete(deletename)){
            System.out.println("succuss");
            return;
        }
        System.out.println("failed");

    }

    private void mkdir(String dirname) throws RemoteException {
        if(dirname.contains("/") && !dirname.endsWith("/")){
            System.out.println("failed");
            return;
        }
        dirname += dirname.endsWith("/")?"":"/";
        Manager.mkdir(dirname);
        System.out.println("success");

    }

    private void rename(String oldName,String newName) throws RemoteException {
        if(oldName.contains("/") != newName.contains("/")){
            System.out.println("without(with) /");
            return;
        }
        if(!Manager.rename(oldName,newName)){
            System.out.println("failed");
        }
    }

    private boolean cd(String tar) throws RemoteException {
        if(!Manager.cd(tar)){
            System.out.println("not exist");
            return  false;
        }
        return  true;
    }

    private void ls() throws RemoteException {
        System.out.println(Manager.ls());
    }

    private void pwd()throws RemoteException {
        System.out.println(Manager.pwd());
    }


    private String NameNodeServerHost = "localhost";
    private int NameNodeServerPort = 8088;

    private Client(String host, int port){
        NameNodeServerHost = host;
        NameNodeServerPort = port;
    }

    private Client(){

    }


    public static void main(String[] args) {
        Client app;
        if(args.length == 2) {
            String host = args[0];
            int port = Integer.parseInt(args[1]);
            if (port < 8001 || port > 9999) {
                System.out.println("args error!");
                return;
            }
            app = new Client(host,port);
        }
        else{
            app = new Client();
        }
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry(app.NameNodeServerHost,app.NameNodeServerPort);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        try {
            app.Manager = (ClientProtocol) registry.lookup("RpcServer");
            String now = "/";
            Scanner sc = new Scanner(System.in);
            while(true){
                System.out.println(now + ":");
                String s = sc.nextLine();
                String[] ss = s.trim().split(" ");
                switch (ss[0]) {
                    case "create":
                        if(app.create(ss[1], ss[2])){
                            System.out.println("Success!");
                        }
                        else{
                            System.out.println("Failed!");
                        }
                        break;
                    case "delete":
                        app.delete(ss[1]);
                        break;
                    case "rename":
                        app.rename(ss[1],ss[2]);
                        break;
                    case "mkdir":
                        app.mkdir(ss[1]);
                        break;
                    case "pwd":
                        app.pwd();
                        break;
                    case "download":
                        app.download(ss[1],ss[2]);
                        break;
                    case "cd":
                        String tar = ss[1];
                        if(app.cd(tar)){
                            String [] tarsplit = tar.split("/");
                            now = tarsplit[tarsplit.length - 1] + "/";
                        }
                        break;
                    case "ls":
                        app.ls();
                        break;
                    case "show":
                        app.Manager.printTree();
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

}