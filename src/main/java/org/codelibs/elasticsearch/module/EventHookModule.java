package org.codelibs.elasticsearch.module;

import org.codelibs.elasticsearch.service.EventHookService;
import org.elasticsearch.common.inject.AbstractModule;

public class EventHookModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(EventHookService.class).asEagerSingleton();
    }
}