import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.rmi.RemoteException;

abstract class BaseDataNode extends DataNodeResponder {
    int dataNodeId;
    DataNodeProtocol Manager = null;
    private List<BlockInfo> blockInfos = new ArrayList<>();
    private Map<String,BlockInfo> blockMaps = new HashMap<>();
    Heart heart;
    BlockReportor reportor;
    int port;
    String url;

    BaseDataNode(){
        try {
            url = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    String dir = "";
    protected class Heart extends Thread{
        Heart(){
            super("heart");
        }

        @Override
        public void run(){
            while(true){
                try {
                    DataNodeResponse[] response = Manager.heartbeat(dataNodeId);
                    solveResponse(response);
                }catch(RemoteException e){
                    e.printStackTrace();
                    break;
                }
                try {
                    sleep(300);
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
            }
        }

    }


    protected class BlockReportor extends Thread{
        BlockReportor(){
            super("BlockReportor");
        }


        public void run(){
            while(true){
                solveDeleteList();
                report();
                try {
                    sleep(1000 * 60);
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
            }
        }

        synchronized void report(){
            try {
                DataNodeResponse[] response = Manager.updateBlockInfo(dataNodeId, blockInfos);
                solveResponse(response);
            }catch(RemoteException e){
                e.printStackTrace();
            }

        }
    }

    protected class SocketTransor extends Thread{
        private Socket socket;

        SocketTransor(Socket socket){
            super("SocketTransor");
            this.socket =socket;
        }


        public void run(){
            try {
                InputStream inputStream = socket.getInputStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                String info = bufferedReader.readLine();
                if(info.equals("download")) {
                    info = bufferedReader.readLine();
                    System.out.println("sending "+ info);
                    OutputStream os = socket.getOutputStream();
                    PrintWriter pw = new PrintWriter(os);
                    if(blockMaps.containsKey(info)) {
                        pw.write("OK\n");
                    }
                    else {
                        pw.write("BAD\n");
                    }
                    pw.flush();
                    File f = null;
                    FileInputStream fi = null;
                    try {
                        f = new File(dir+info);
                        fi = new FileInputStream(f);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    if(f == null || fi == null){
                        System.out.println("not exist"+dir+info);
                        return;
                    }
                    char[] buf = new char[MACRO.BLOCK_LENGTH];
                    BufferedReader br = new BufferedReader(new InputStreamReader(fi));
                    br.read(buf,0,MACRO.BLOCK_LENGTH);
                    pw.write(buf);
                    pw.flush();
                    os.close();
                }
                else if(info.equals("update")){
                    info = bufferedReader.readLine();
                    char[] buf = new char[MACRO.BLOCK_LENGTH];
                    bufferedReader.read(buf,0,MACRO.BLOCK_LENGTH);
                    createBlock(info, buf);
                    bufferedReader.close();
                    inputStream.close();
                }
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void createBlock(String info,char[] buf){
        try {
            File workDir=new File(dir);
            if(!workDir.exists()){//如果文件夹不存在
                workDir.mkdir();//创建文件夹
            }
            File f = new File(dir+info);
            if(!f.exists()){
                f.createNewFile();
                System.out.println("create "+info);
            }
            FileOutputStream fo = new FileOutputStream(f);
            PrintWriter pw = new PrintWriter(fo);
            pw.write(buf,0,MACRO.BLOCK_LENGTH);
            pw.flush();
            BlockInfo binfo = new BlockInfo();
            binfo.blockId = Integer.parseInt(info.trim().split(":")[1]);
            binfo.file = INCOMPLETE_FILE(info.trim().split(":")[0]);
            blockMaps.put(binfo.makeKey(),binfo);
            blockInfos.add(binfo);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public class BlockSender extends Thread implements Serializable{
        private String blockname;
        private String url;
        private int port;
        BlockSender(String blockname,String url, int port){
            super("BlockSender");
            this.blockname = blockname;
            this.url = url;
            this.port = port;
        }

        public void run(){
            Socket s = null;
            try {
                s = new Socket(url,port);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(s == null){
                return;
            }
            try {
                OutputStream os = s.getOutputStream();
                PrintWriter pw = new PrintWriter(os);
                pw.write("download\n"+blockname+"\n");
                pw.flush();
                InputStream inputStream = s.getInputStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                String info = bufferedReader.readLine();
                if(info.equals("OK")) {
                    char[] buf = new char[MACRO.BLOCK_LENGTH];
                    bufferedReader.read(buf, 0, MACRO.BLOCK_LENGTH);
                    createBlock(blockname, buf);
                    bufferedReader.close();
                    inputStream.close();
                }

                else{
                    System.out.println("Copy from None:"+info);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
    private void solveResponse(DataNodeResponse[] response){
        if(response == null){
            return;
        }
        for(DataNodeResponse r : response){
            if(r == null){
                break;
            }
            switch (r.msgType){
                case -1:
                    System.out.println("Error Msg: -1!");
                    break;
                case 0:
                    reportor.report();
                    break;
                case 1:
                    CopyResponse cr = (CopyResponse)r;
                    BlockSender blockSender = new BlockSender(cr.blockName,cr.url,cr.port);
                    blockSender.start();
                    break;
                case 2:
                    DeleteResponse dr = (DeleteResponse)r;
                    deleteList.add(dr.blockName);
                    break;
            }
        }
    }

    private List<String> deleteList = new ArrayList<>();

    private void solveDeleteList(){
        for(Iterator<BlockInfo> it = blockInfos.iterator();it.hasNext();) {
            BlockInfo b = it.next();
            if(deleteList.contains(b.makeKey())){
                File f = new File(b.makeKey());
                if(f.exists()){
                    f.delete();
                }
                it.remove();
            }
        }
        deleteList.clear();
    }
}
