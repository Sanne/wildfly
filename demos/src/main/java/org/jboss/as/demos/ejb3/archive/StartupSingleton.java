/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2010, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.as.demos.ejb3.archive;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.LocalBean;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.List;

/**
 * @author Jaikiran Pai
 */
@Startup
@Singleton
@LocalBean
public class StartupSingleton {

    private List<String> invocationLog;

    @Resource(lookup = "java:module/ModuleName")
    private String moduleName;

    public static final String STARTUP_SINGLETON_POST_CONSTRUCT = "StartupSingleton_PostConstruct";

    @PostConstruct
    public void postConstruct() throws Exception {
        Context ctx = new InitialContext();
        CallTrackerSingletonBean callTrackerSingletonBean = (CallTrackerSingletonBean) ctx.lookup("java:global/" + moduleName + "/" + CallTrackerSingletonBean.class.getSimpleName() + "!" + CallTrackerSingletonBean.class.getName());
        callTrackerSingletonBean.addToInvocationLog(STARTUP_SINGLETON_POST_CONSTRUCT);
    }

}
