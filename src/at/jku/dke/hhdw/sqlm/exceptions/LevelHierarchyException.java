package at.jku.dke.sqlm.exceptions;


public class LevelHierarchyException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	public LevelHierarchyException(String reason, int vendorCode, int errorLine) {
		super("LevelHierarchyException:\nORA-"+ vendorCode + ": " + reason+
				"\nSQL-M: Error occured before line "+errorLine);
	}
	
	public LevelHierarchyException(String reason){
	    super("LevelHierarchyException: " + reason);
	}

	public String toString(){
		return super.toString();
	}

}
