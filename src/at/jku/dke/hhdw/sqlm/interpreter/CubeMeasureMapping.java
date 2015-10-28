package at.jku.dke.sqlm.interpreter;

import java.util.LinkedList;

public class CubeMeasureMapping {
	String mcubeName;
	LinkedList<String> measures;
	
	public CubeMeasureMapping(){
		measures = new LinkedList<String>();
	}
	
	public void setCubeName(String cubeName){
		mcubeName = cubeName;
	}
	
	public String getCubeName(){
		return mcubeName;
	}
	
	public void addMeasure(String measure){
		measures.add(measure);
	}
	
	public String getMeasure(int pos){
		return measures.get(pos);
	}
	
	public boolean containsMeasure(String measure){
		boolean contains = false;
		if(measures.size() == 0){
			return contains;
		}
		else{
			for(int i=0;i<measures.size();i++){
				if(measures.get(i).equals(measure)){
					contains = true;
				}
			}
		}
		return contains;
	}
	
	public int measureCount(){
		return measures.size();
	}
}
