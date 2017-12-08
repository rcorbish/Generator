
package com.rc.trigger ; 

@FunctionalInterface
public interface TriggerEventListener {

    public void fire( Event e ) ;

}