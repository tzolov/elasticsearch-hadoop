/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.cfg;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;

public class HadoopSettings extends Settings {

    private final Configuration cfg;

    public HadoopSettings(Configuration cfg) {
        Validate.notNull(cfg, "Non-null properties expected");
        this.cfg = cfg;
    }

    @Override
    public String getProperty(String name) {
        return cfg.get(name);
    }

    @Override
    public void setProperty(String name, String value) {
        cfg.set(name, value);
    }
}
