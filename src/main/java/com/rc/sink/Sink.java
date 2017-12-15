
package com.rc.sink ;

public interface Sink {

	public void header( CharSequence data ) ;
	public void consume( CharSequence data ) ;
	public void footer( CharSequence data ) ;
    
}