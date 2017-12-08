
package com.rc;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rc.source.Source;
import com.rc.transformer.Transformer;
import com.rc.trigger.Event;
import com.rc.trigger.Trigger;
import com.rc.trigger.TriggerEventListener;

public class Processor implements TriggerEventListener, AutoCloseable {

    final static Logger log = LoggerFactory.getLogger(Processor.class);

    final private Source source;
    final private List<Transformer> transformers;
    final private Trigger trigger;
    final public String name;

    public Processor(String name, Source source) {
        this(name, source, null);
    }

    public Processor(String name, Source source, Trigger trigger) {
        this.name = name;
        this.source = source;
        this.transformers = new ArrayList<Transformer>();
        this.trigger = trigger;
    }

    public void addTransformer(Transformer transformer) {
        transformers.add(transformer);
    }

    public void start() {
        if (trigger != null) {
            trigger.addListener(this);
            trigger.start();
            log.info("Processor started");
        } else {
            log.info("No trigger defined for {}, not waiting", name);
        }
    }

    @Override
    public void fire(Event e) {
        log.info("Processor {} triggered", name);
        try {
            Stream<Object[]> rawData = source.get();

            for (Transformer transformer : transformers) {
                transformer.process(rawData);
            }
            log.info("Completed processing of {}", name);
        } catch (Throwable t) {
            log.error("Failed during processing", t);
        }
    }

    @Override
    public void close() {
        if (trigger != null) {
            trigger.removeListener(this);
        }
        log.info("Processor shutdown");
    }

}