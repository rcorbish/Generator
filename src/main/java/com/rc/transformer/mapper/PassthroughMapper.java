
package com.rc.transformer.mapper ;

public class PassthroughMapper implements Mapper {

	final int sourceIndex ;
	
	public PassthroughMapper( final int sourceIndex ) {
		this.sourceIndex = sourceIndex ;
	}
	
    public String process( Object data[] ) {
    	return data[sourceIndex] == null ? "" : data[sourceIndex].toString() ;
    }
    
}