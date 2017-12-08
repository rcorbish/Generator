
package com.rc.trigger ;

import java.util.LinkedList;
import java.util.List;

abstract public class Trigger {

    private final List<TriggerEventListener> listeners ;
    protected final String name ;

    public Trigger( String name ) {
        listeners = new LinkedList<>() ;
        this.name = name ;
    }

    public void addListener( TriggerEventListener tel ) {
        listeners.add( tel ) ;
    }

    public void removeListener( TriggerEventListener tel ) {
        listeners.remove( tel ) ;
    }
    
    abstract public void start() ;
    abstract public void stop() ;
    
    public void fire( Event e ) {
        for( TriggerEventListener tel : listeners ) {
            tel.fire( e ) ;
        }
    }

}