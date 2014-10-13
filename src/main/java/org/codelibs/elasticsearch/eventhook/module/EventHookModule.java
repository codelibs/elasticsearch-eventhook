package org.codelibs.elasticsearch.eventhook.module;

import org.codelibs.elasticsearch.eventhook.service.EventHookService;
import org.elasticsearch.common.inject.AbstractModule;

public class EventHookModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(EventHookService.class).asEagerSingleton();
    }
}