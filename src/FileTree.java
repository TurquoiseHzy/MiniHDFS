
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FileTree implements Serializable{
    abstract class TreeNode implements Serializable{
        DirTreeNode father = null;
        String name;
        TreeNode(String name){
            this.name = name;
        }
    }

    class DirTreeNode extends TreeNode implements Serializable{
        Map<String,TreeNode> children = new HashMap<String,TreeNode>();
        DirTreeNode(DirTreeNode father,String name){
            super(name);
            this.father = father;
        }
        TreeNode getChild(String key){
            return children.get(key);
        }

        String pwd(){
            String result = "";
            DirTreeNode now = this;
            while(now != null){
                result = now.name + result;
                now = now.father;
            }
            return result;
        }

        String ls(){
            String result = "";
            for(String s : this.children.keySet()){
                result = result + s + "\n";
            }
            return result;
        }

    }

    class FileTreeNode extends TreeNode implements Serializable{
        DataNodeResponder.FileInfo info = null;
        FileTreeNode(DirTreeNode father,String name){
            super(name);
            this.father = father;
        }

        void remove(){
            this.father.children.remove(this.name);
        }

    }


    boolean cd(String tar){
        if(tar.equals("."))
            return true;
        if(tar.equals("..")) {
            if(workDir.father != null) {
                workDir = workDir.father;
                return true;
            }
            else{
                return false;
            }
        }
        if(!tar.endsWith("/")){
            tar = tar + "/";
        }
        TreeNode node = get(tar);
        System.out.println(node);
        if(node == null || node.getClass() != DirTreeNode.class)
            return false;
        else {
            workDir = (DirTreeNode)node;
            return true;
        }
    }


    DirTreeNode rootDir = new DirTreeNode(null,"/");
    DirTreeNode workDir = rootDir;

    TreeNode insertFile(String dirname){

        dirname = dirname.trim();
        if(dirname.contains(" ")){
            return null;
        }

        String[] names = dirname.split("/");
        int i = dirname.startsWith("/") ? 1 : 0;
        DirTreeNode now = dirname.startsWith("/") ? rootDir : workDir;
        DirTreeNode child;
        int maxi = names.length - (dirname.endsWith("/")?0:1);
        while(i < maxi){
            names[i] =names[i] +"/";
            if((child = (DirTreeNode) now.getChild(names[i]) )!= null){
                now = child;
            }
            else{
                child = new DirTreeNode(now,names[i]);
                now.children.put(names[i],child);
                now = child;
            }
            i++;
        }
        if(dirname.endsWith("/")){
            return now;
        }
        if(now.getChild(names[i]) != null){
            return null;
        }
        else{
            FileTreeNode file = new FileTreeNode(now,names[i]);
            now.children.put(names[i], file);
            return file;
        }
    }

    TreeNode get(String dirname){
        dirname = dirname.trim();
        if(dirname.contains(" ")){
            return null;
        }
        String[] names = dirname.split("/");
        int i = (dirname.startsWith("/")) ? 1 : 0;
        DirTreeNode now = (dirname.startsWith("/")) ? rootDir : workDir;
        DirTreeNode child;
        int maxi = names.length - (dirname.endsWith("/")?0:1);
        while(i < maxi){
            names[i] =names[i] +"/";
            if((child = (DirTreeNode) now.getChild(names[i]) )!= null){
                now = child;
                i++;
            }
            else{
                return null;
            }
        }
        if(dirname.endsWith("/")){
            return now;
        }
        else{
            return now.getChild(names[i]);
        }
    }

    boolean rename(String dirname,String newName){
        TreeNode node;
        if(newName.substring(0,newName.length() - 1).contains("/")){
            return false;
        }
        if((node = get(dirname)) != null){
            if(node.equals(rootDir)){
                return false;
            }
            if(node.getClass() == DirTreeNode.class){
                if(newName.contains(".")||newName.contains(":")){
                    return false;
                }
                else{
                    node.father.children.remove(node.name);
                    node.name = newName +(newName.endsWith("/")?"":"/");
                    node.father.children.put(node.name,node);
                    return true;
                }
            }
            else{
                if(newName.contains("/")){
                    return false;
                }
                else{
                    node.father.children.remove(node.name);
                    node.name = newName;
                    node.father.children.put(node.name,node);
                    return true;
                }
            }
        }
        else{
            return false;
        }
    }

    static FileTree readXML(File f){
        FileTree tree = new FileTree();
        Document document = null;
        try {
            SAXReader saxReader = new SAXReader();
            document = saxReader.read(f); // 读取XML文件,获得document对象
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        if(document == null){
            return null;
        }
        Element element = document.getRootElement();
        if(!element.getName().equals("FileTree")){
            return null;
        }
        List<Element> root = element.elements();
        if(root.size() != 1)
        {
            return null;
        }
        for(Element e : root){

            if(!tree.buildTreeNode(e, tree.rootDir)){
                return null;
            }
        }
        return tree;
    }

    private boolean buildTreeNode(Element element,DirTreeNode father){
        for(Iterator<Element> iter = element.elementIterator();iter.hasNext();){
            Element el = iter.next();
            String cla = el.getName();
            String name = el.attributeValue("name");
            switch(cla){
                case "DirTreeNode":
                    DirTreeNode dnode = new DirTreeNode(father,name);
                    father.children.put(name,dnode);
                    buildTreeNode(el,dnode);
                    break;
                case "FileTreeNode":
                    FileTreeNode fnode = new FileTreeNode(father,name);
                    father.children.put(name,fnode);
                    break;
                    default:
                        return false;
            }
        }
        return  true;
    }

    void writeXML(File f) throws IOException {
        Document doc
                = DocumentHelper.createDocument();
        Element rootElement = doc.addElement("FileTree");
        buildElement(rootElement,rootDir);
        // 设置XML文档格式
        OutputFormat outputFormat = OutputFormat.createPrettyPrint();
        outputFormat.setEncoding("UTF-8");
        outputFormat.setIndent(true);
        outputFormat.setIndent("    ");
        outputFormat.setNewlines(true);
        XMLWriter writer = new XMLWriter(new FileWriter(f),outputFormat);
        writer.write(doc);
        writer.close();
    }

    private  void buildElement(Element fatherElement, TreeNode node){
        Element result = fatherElement.addElement(node.getClass().getSimpleName());
        result.addAttribute("name",node.name);
        if(node.getClass() == DirTreeNode.class){
            for(TreeNode child : ((DirTreeNode)node).children.values()) {
                buildElement(result, child);
            }
        }
    }

    public static void main(String args[]) throws IOException {
        FileTree fileTree = new FileTree();
        fileTree.insertFile("adc/");
        fileTree.insertFile("/adcb/");
        fileTree.insertFile("/adcb/");

        fileTree.insertFile("/adcb/");

        fileTree.insertFile("/adcb/x");
        fileTree.cd("adcb");
        fileTree.insertFile("adcb");
        File f = new File("tree.xml");
        if(!f.exists()){
            f.createNewFile();
        }
        fileTree.writeXML(f);
    }
}
