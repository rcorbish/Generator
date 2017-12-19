
package com.rc.transformer.mapper ;

import java.util.Date;

public class PassthroughMapper implements Mapper {

	final int sourceIndex ;
	
	public PassthroughMapper( final int sourceIndex ) {
		this.sourceIndex = sourceIndex ;
	}
	
    public String process( Object data[] ) {
		Object o = data[sourceIndex] ;
		if( o instanceof Date ) {
			// format a date
		}
    	return o == null ? "" : o.toString() ;
    }
    
}