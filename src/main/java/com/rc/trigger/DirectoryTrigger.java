
package com.rc.trigger ;

import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DirectoryTrigger extends Trigger {

    final static Logger log = LoggerFactory.getLogger( DirectoryTrigger.class ) ;

    private final Path watchDirectory ;    
    private final Thread fileWatchThread ;

    public DirectoryTrigger( Path watchDirectory ) {
        super( "DIR: " + watchDirectory ) ;
        this.watchDirectory = watchDirectory ;
        fileWatchThread = new Thread( this::waitForFiles ) ;
    }

    private void waitForFiles() {
        fileWatchThread.setName( name ) ;
        log.info( "Trigger waiting for changes in {}", watchDirectory ) ;
        try {
            for( ; ; ) {
                Thread.sleep( 3713 ) ;
                
                Event e = new Event( name ) ;
                e.data = watchDirectory ;
                fire( e ) ;
            }
        } catch( InterruptedException ignore ) {
            log.info( "Trigger interrupted" ) ;
        }
    }

    @Override
    public void start() {
        fileWatchThread.start(); 
    }

    @Override
    public void stop() {
        if( fileWatchThread.isAlive() ) {
            fileWatchThread.interrupt();
        }
    }
}