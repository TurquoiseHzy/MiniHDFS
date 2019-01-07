import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Test {
    static class A{
        public B b;
        public int val = 0;

    }

    static class B{
        public A a;
        public int val = 0;
    }

    public static void main(String args[]) throws IOException {
        File f =new File("info.txt:0");
        f.createNewFile();

        File f2 = new File("info.txt:0");
        if(f2.exists()){
            System.out.println("!!!!");
        }
    }

}
