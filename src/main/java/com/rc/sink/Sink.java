
package com.rc.sink ;

public interface Sink {

	public void header( String data ) ;
	public void consume( Object data[] ) ;
	public void footer( String data ) ;
    
}