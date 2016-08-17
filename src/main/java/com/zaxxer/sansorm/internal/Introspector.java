/*
 * The contents of this file are proprietary and may not be
 * modified or distributed in source or compiled form without
 * express written permission from the copyright holders.
 *
 * Copyright 2008-2011, DancerNetworks
 * All rights reserved.
 */

package com.zaxxer.sansorm.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class Introspector {
    private static final Map<Class<?>, Introspected> descriptorMap = new ConcurrentHashMap<>();


    private Introspector() {}

    public static Introspected getIntrospected(Class<?> clazz) throws InstantiationException, IllegalAccessException {
        Introspected introspected = descriptorMap.get(clazz);
        if (introspected != null) {
            return introspected;
        }

        // Introspection should only occur once per class.
        synchronized (clazz) {
            // Double check.  This avoids multiple introspections of the same class.
            introspected = descriptorMap.get(clazz);
            if (introspected != null) {
                return introspected;
            }

            introspected = new Introspected(clazz);
            descriptorMap.put(clazz, introspected);
            return introspected;
        }
    }
}
