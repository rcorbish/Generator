
package com.rc.trigger ;

import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimeTrigger extends Trigger {

    final private static Logger log = LoggerFactory.getLogger( TimeTrigger.class ) ;
    
    final private int hour ;
    final private int minute ;
    final private int daysOfWeek[] ;
    final private TimeZone tz ;
    final private Thread thread ;

    public TimeTrigger( int hour, int minute, TimeZone tz, int ... daysOfWeek ) {
        super( String.format( "WAIT %02d:%02d", hour, minute ) ) ;
        this.hour = hour ;
        this.minute = minute ;
        this.tz = tz ;
        this.daysOfWeek = new int[ daysOfWeek.length ] ;
        System.arraycopy( this.daysOfWeek, 0, daysOfWeek, 0, daysOfWeek.length );
        Arrays.sort( daysOfWeek ) ;
        thread = new Thread( this::waitForTime ) ;
    }

    private void waitForTime() {
        thread.setName( name ) ;
        log.info( "Trigger started" ) ;
        try {
            for( ; ; ) {
                Calendar cal = Calendar.getInstance() ;
                cal.setTimeZone( tz ) ;
                cal.set( Calendar.HOUR_OF_DAY, hour ) ;
                cal.set( Calendar.MINUTE, minute ) ;
                long sleepTime = 0 ;
                do {
                    int dow = cal.get( Calendar.DAY_OF_WEEK ) ;
                    while( Arrays.binarySearch( daysOfWeek, dow ) < 0 ) {
                        cal.add(Calendar.DAY_OF_YEAR, 1 ) ;
                        dow = cal.get( Calendar.DAY_OF_WEEK ) ;
                    }

                    sleepTime = cal.getTimeInMillis() - System.currentTimeMillis() ;
                    cal.add(Calendar.DAY_OF_YEAR, 1 ) ;
                } while( sleepTime < 0 ) ;
                Thread.sleep( sleepTime ) ;

                log.info( "Time elapsed ... firing trigger" ) ;
                Event e = new Event( name ) ;
                e.data = "Timeout" ;
                fire( e ) ;
            }
        } catch( InterruptedException ignore ) {
            log.warn( "Trigger interrupted" ) ;
        }
    }

    @Override
    public void start() {
        thread.start(); 
    }

    @Override
    public void stop() {
        if( thread.isAlive() ) {
            thread.interrupt();
        }
    }
}