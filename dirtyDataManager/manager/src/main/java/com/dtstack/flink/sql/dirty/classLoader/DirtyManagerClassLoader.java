/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.dirty.classLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/27 星期四
 */
public class DirtyManagerClassLoader extends URLClassLoader {
    private final Logger LOG = LoggerFactory.getLogger(DirtyManagerClassLoader.class);

    /**
     * The parent class loader.
     */
    protected ClassLoader parent;

    public DirtyManagerClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
        this.parent = parent;
    }


    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return this.loadClass(name, false);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("loadClass(" + name + ", " + resolve + ")");
            }
            Class<?> clazz;

            // (0.1) Check our previously loaded class cache
            clazz = findLoadedClass(name);
            if (clazz != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("  Returning class from cache");
                }
                if (resolve) {
                    resolveClass(clazz);
                }
                return (clazz);
            }

            // (2) Search local repositories
            if (LOG.isDebugEnabled()) {
                LOG.debug("  Searching local repositories");
            }
            try {
                clazz = findClass(name);
                if (clazz != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("  Loading class from local repository");
                    }
                    if (resolve) {
                        resolveClass(clazz);
                    }
                    return (clazz);
                }
            } catch (ClassNotFoundException e) {
                // Ignore
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("  Delegating to parent classloader at end: " + parent);
            }

            try {
                clazz = Class.forName(name, false, parent);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("  Loading class from parent");
                }
                if (resolve) {
                    resolveClass(clazz);
                }
                return (clazz);
            } catch (ClassNotFoundException e) {
                // Ignore
            }
        }

        throw new ClassNotFoundException(name);
    }
}
