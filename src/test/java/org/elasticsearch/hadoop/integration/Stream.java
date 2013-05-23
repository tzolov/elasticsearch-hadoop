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
package org.elasticsearch.hadoop.integration;

import java.io.PrintStream;

public enum Stream {
    OUT {
        @Override
        public PrintStream stream() {
            return System.out;
        }
    },
    ERR {
        @Override
        public PrintStream stream() {
            return System.err;
        }
    },
    NULL {
        @Override
        public PrintStream stream() {
            return new NullPrintStream();
        }
    };

    public abstract PrintStream stream();
}