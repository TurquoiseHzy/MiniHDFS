public class Test {
    static class A{
        public B b;
        public int val = 0;

    }

    static class B{
        public A a;
        public int val = 0;
    }

    public static void main(String args[]){
        A x = new A();
        x.val = 1 ;
        x.b = new B();
        x.b.a = x;
        System.out.println(x.b.a.b.a.b.a.val);
    }

}
