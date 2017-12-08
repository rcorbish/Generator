
package com.rc.trigger ;

public class Event {
    Object data ;
    long timestamp ;
    String triggerName ;

    public Event( String triggerName ) {
        timestamp = System.currentTimeMillis() ;
        this.triggerName = triggerName ;
    }
}