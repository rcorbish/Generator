
package com.rc.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rc.Processor;
import com.rc.sink.FileSink;
import com.rc.sink.Sink;
import com.rc.source.FileSource;
import com.rc.source.Source;
import com.rc.transformer.Passthrough;
import com.rc.transformer.Transformer;
import com.rc.trigger.TimeTrigger;
import com.rc.trigger.Trigger;

public class Config {

    final static Logger log = LoggerFactory.getLogger(Config.class);
    final Path configFile;

    private Source source ;
    private Trigger trigger ;
    private Sink sink  ;
    private Transformer transformer ;

    public Config(Path configFile) {
        this.configFile = configFile;
    }

    public Processor createProcessor() throws IOException, ParseException {
        log.info("Creating processor - {}", configFile.getFileName());

        parseConfigFile() ;

        if( source == null ) throw new ParseException("No source found", -1) ;
        if( sink == null ) throw new ParseException("No sink found", -1) ;
        if( transformer == null ) throw new ParseException("No transformer found", -1) ;
        Processor rc = new Processor(configFile.getFileName().toString(), source, trigger);
        transformer.sink = sink ;
        rc.addTransformer(transformer);
 
        return rc;
    }

    public Config parseConfigFile() throws ParseException {
        Config rc = null ;

        String section = null ;
        Map<String,String> values = new LinkedHashMap<>() ;
        
        log.info("Parsing config file {}", configFile ) ;


        try( BufferedReader rdr = Files.newBufferedReader( configFile ) ) {
            // Gson gson = new GsonBuilder().setLenient().create() ;
            // rc = gson.fromJson( rdr, Config.class ) ;
            int lineNum = 1 ;
            String line = "" ;

            for( String s=rdr.readLine() ; s!=null ; s=rdr.readLine(), lineNum++ ) {
                s = s.trim() ;
                if( s.length()==0 || s.charAt(0) == '#' ) continue ;
                if( s.charAt(0) == '[' ) {
                    int ix = s.indexOf(']') ;
                    if( ix < 0 ) {
                        throw new ParseException( "Bad section formatting", lineNum ) ;
                    }
                    processSection( section, values ) ;
                    values.clear();
                    
                    section = s.substring(1, s.indexOf(']') ).trim().toLowerCase() ;
                } else if( s.charAt( s.length()-1) == '\\' ) {
                    line += s ;
                } else {
                    line += s ;
                    int ix = 0 ;
                    while( ix<line.length() && 
                            !Character.isWhitespace(line.charAt(ix)) ) {
                        ix++ ;
                    }
                    String key = line.substring(0,ix) ;
                    String value = line.substring(ix).trim() ;
                    values.put( key, value ) ;
                    line = "" ;
                }
            } 
            processSection( section, values ) ;
        } catch (IOException iex) {
            log.error( "Can't create processor: ", iex ) ;
            rc = null;
        }
        return rc;
    }

    public void processSection( String sectionName, Map<String,String> values ) throws IOException, ParseException {
        if( sectionName !=null ) {
            log.info( "Processed {} using {}", sectionName, values ) ;

            if( "source".equals( sectionName ) ) {
                processSource( values ) ;
            }
            if( "sink".equals( sectionName ) ) {
                processSink( values ) ;
            }
            if( "transformer".equals( sectionName ) ) {
                processTransformer( values ) ;
            }
            if( "trigger".equals( sectionName ) ) {
                processTrigger( values ) ;
            }
        }
    }

    public void processSource(  Map<String,String> values ) throws IOException, ParseException {
        source = new FileSource(Paths.get("/etc/passwd"), ":");
    }
    public void processSink(  Map<String,String> values ) throws IOException, ParseException  {
        Path output = null ;
        String fileName = values.get( "file" ) ;
        if( fileName != null ) {
            output = FileSink.getFileNameFromFile(fileName) ;
        }
        String dirName = values.get( "dir" ) ;
        if( dirName != null ) {
            if( fileName != null ) {
                throw new ParseException("Don't use file and dir in the same config", -1) ;
            }
            output = FileSink.getFileNameFromDir(dirName) ;
        }
        if( output != null ) {
            sink = new FileSink( Paths.get( fileName ) ) ;
        }
    }
    public void processTransformer(  Map<String,String> values ) throws IOException, ParseException  {
        transformer = new Passthrough() ;       
    }
    public void processTrigger(  Map<String,String> values ) throws IOException {
        trigger = new TimeTrigger(20, 0, TimeZone.getDefault(), Calendar.SUNDAY);
    }
}
