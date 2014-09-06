package org.codelibs.elasticsearch;

import java.util.Collection;

import org.codelibs.elasticsearch.module.EventHookModule;
import org.codelibs.elasticsearch.service.EventHookService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;

public class EventHookPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "EventHookPlugin";
    }

    @Override
    public String description() {
        return "This is a elasticsearch-eventhook plugin.";
    }

    // for Service
    @Override
    public Collection<Class<? extends Module>> modules() {
        final Collection<Class<? extends Module>> modules = Lists
                .newArrayList();
        modules.add(EventHookModule.class);
        return modules;
    }

    // for Service
    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        final Collection<Class<? extends LifecycleComponent>> services = Lists
                .newArrayList();
        services.add(EventHookService.class);
        return services;
    }
}
