package at.jku.dke.sqlm.parser;

import java.util.ArrayList;

public class SQLMNode extends SimpleNode {

	public SQLMNode(int id) {
		super(id);
	}

	public SQLMNode(SQLMParser p, int id) {
		super(p, id);
	}

	public void jjtDeleteChild(int i) {
		  Node c[] = new Node[children.length - 1];	  
		  if(i>0){
			  System.arraycopy(children, 0, c, 0, i);
		  }
		  if(i<children.length){
			  System.arraycopy(children, i+1, c, i, c.length-i);
		  }
		  children = c;
	  }
	  
	  public void jjtSwapChildOnePositionForward(int i) {
		  Node c[] = new Node[children.length];	  
		  if(i>1){
			  System.arraycopy(children, 0, c, 0, i-1);
			  System.arraycopy(children, i, c, i-1, 1);
			  System.arraycopy(children, i-1, c, i, 1);
			  if(children.length>i+1){
				  System.arraycopy(children, i+1, c, i+1, children.length-(i+1));
			  }
		  }else{
			  System.arraycopy(children, 1, c, 0, 1);
			  System.arraycopy(children, 0, c, 1, 1);
			  if(children.length>2){
				  System.arraycopy(children, 2, c, 2, children.length-2);
			  }
		  }
		  children = c;
	  }
	  
	  public SQLMNode jjtGetChild(int i) {
		    return (SQLMNode) children[i];
	  }
	  
	  public SQLMNode jjtGetChild(Class nodeClass) {
		  try{
			  for(int i=0;i<children.length;i++){
				  if(children[i].getClass().equals(nodeClass)){
					  return (SQLMNode)children[i];
				  }
			  }
		  }catch(Exception e){
			  e.printStackTrace(); 
		  }
		  return null;
	  }
	  
	  public void jjtInsertChild(Node n, int i) {
		  if (children == null) {
			  children = new Node[i + 1];
		    } else if (i >= children.length) {
		      Node c[] = new Node[i + 1];
		      System.arraycopy(children, 0, c, 0, children.length);
		      children = c;
		    } else if (i < children.length) {
		    	Node c[] = new Node[children.length + 1];
		    	System.arraycopy(children, 0, c, 0, i);
		        System.arraycopy(children, i, c, i+1, children.length-i);
		        children = c;
		    }
		    children[i] = n;
	  }
	  
	  public void jjtSetNodeNull(int pos){
		  children[pos] = null;
	  }
	  
	  public void jjtClearEmptyChildNodes(){
		  int newLength = 0;
		  for(int i=0; i < children.length; i++){
			  if(children[i] != null){
				  newLength++;
			  }
		  }
		  Node c[] = new Node[newLength];
		  int pos = 0;
		  for(int i=0; i < children.length; i++){
			  if(children[i] != null){
				  c[pos] = children[i];
				  pos++;
			  }
		  }
		  children = c;
	  }
	  
	  public void jjtSetErrorLine(int error){
		  this.errorLine = error;
	  }
	  
	  public int jjtGetErrorLine(){
		  return this.errorLine;
	  }
	  
	  public void jjtAddChild(ArrayList<Node> list, int i) {
		  if (children == null) {
			  children = new Node[i + list.size()];
		  } else if (i >= children.length) {
		      Node c[] = new Node[i + list.size()];
		      System.arraycopy(children, 0, c, 0, children.length);
		      children = c;
		  }
		  for(int j=0;j<list.size();j++){
			  children[(i+j)] = list.get(j);
		  }
		  
	  }
	  
	  public void setLengthOfChildren(int length){
		  children = new Node[length];
	  }
	  
	  public void jjtAddChildBlind(Node n, int i) {
		  children[i] = n;
	  }
	
}
