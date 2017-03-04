/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kinesis;

import static java.util.Objects.requireNonNull;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.inject.Injector;
import io.airlift.log.Logger;

import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Kinesis version of Presto Plugin interface.
 *
 * The connector manager injects the type manager and node manager, and then calls getServices
 * to get the connector factory.
 */
public class KinesisPlugin
        implements Plugin
{
    private static final Logger log = Logger.get(KinesisPlugin.class);

    private Optional<Supplier<Map<SchemaTableName, KinesisStreamDescription>>> tableDescriptionSupplier = Optional.empty();
    private Map<String, String> optionalConfig = ImmutableMap.of();
    private Optional<Class<? extends KinesisClientProvider>> altProviderClass = Optional.empty();

    private KinesisConnectorFactory factory;

    public synchronized void setOptionalConfig(Map<String, String> optionalConfig)
    {
        // Note: this used to be a method in older versions of the plugin SPI
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        if (this.factory == null) {
            log.info("Creating connector factory.");
            this.factory = new KinesisConnectorFactory(tableDescriptionSupplier, optionalConfig, altProviderClass);
        }
        return ImmutableList.of(this.factory);
    }

    @VisibleForTesting
    public synchronized void setTableDescriptionSupplier(Supplier<Map<SchemaTableName, KinesisStreamDescription>> tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = Optional.of(requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null"));
    }

    @VisibleForTesting
    public <T extends KinesisClientProvider> void setAltProviderClass(Class<T> aType)
    {
        // Note: this can be used for other cases besides testing but that was the original motivation
        altProviderClass = Optional.of(requireNonNull(aType, "Provider class type is null"));
    }

    @VisibleForTesting
    public synchronized Injector getInjector()
    {
        if (this.factory != null) {
            return this.factory.getInjector();
        }
        else {
            return null;
        }
    }
}
