import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.rmi.RemoteException;
public class BaseDataNode extends DataNodeResponsor {
    private final int MAX_BLOCK_COUNT = 100;
    protected int dataNodeId;
    protected DataNodeProtocol Manager = null;
    protected List<BlockInfo> blockInfos = new ArrayList<>();
    protected Heart heart;
    protected BlockReportor reportor;
    public int port = 9000;
    public String url = "localhost";
    public String dir = "";
    protected class Heart extends Thread{
        public Heart(){
            super("heart");
        }

        @Override
        public void run(){
            while(true){
                try {
                    DataNodeResponse response = Manager.heartbeat(dataNodeId);
                    solveResponse(response);
                }catch(RemoteException e){
                    System.out.println(e);
                    break;
                }
                try {
                    sleep(300);
                }catch(InterruptedException e){
                    System.out.println(e);
                }
            }
        }

    }


    protected class BlockReportor extends Thread{
        public BlockReportor(){
            super("BlockReportor");
        }


        public void run(){
            while(true){
                report();
                try {
                    sleep(1000 * 60);
                }catch(InterruptedException e){
                    System.out.println(e);
                }
            }
        }

        public synchronized void report(){
            try {
                DataNodeResponse response = Manager.updateBlockInfo(dataNodeId,blockInfos);
                solveResponse(response);
            }catch(RemoteException e){
                System.out.println(e);
            }

        }
    }

    protected class SocketTransor extends Thread{
        private Socket socket;

        public SocketTransor(Socket socket ){
            super("SocketTransor");
            this.socket =socket;
        }


        public void run(){
            try {
                InputStream inputStream = socket.getInputStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                String temp = null;
                String info = "";
                if ((temp = bufferedReader.readLine() )!= null){
                    info = info + temp;
                }
                byte []buf = new byte[Client.BLOCK_LENGTH];
                inputStream.read(buf,0,Client.BLOCK_LENGTH);
                createBlock(info,buf);
                bufferedReader.close();
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected void createBlock(String info,byte[] buf){
        try {
            info = dir + info;
            System.out.println(info);
            File f = new File(info);
            if(!f.exists()){
                f.createNewFile();
                System.out.println("create "+info);
            }
            FileOutputStream fo = new FileOutputStream(f);
            fo.write(buf,0,Client.BLOCK_LENGTH);
            BlockInfo binfo = new BlockInfo();
            binfo.copies = 1;
            binfo.blockId = Integer.parseInt(info.trim().split("_")[1]);
            binfo.filename = info.trim().split("_")[0];
            blockInfos.add(binfo);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    protected void solveResponse(DataNodeResponse response){
        if(response == null){
            return;
        }
        switch (response.msgType){
            case 0:
                reportor.report();
        }
    }

}
