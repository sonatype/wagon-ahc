package org.sonatype.maven.wagon;

/*******************************************************************************
 * Copyright (c) 2010-2011 Sonatype, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class UrlUtils
{

    private static final Map<String, String> protocolMap = new HashMap<String, String>();

    static
    {
        protocolMap.put( "http", "http" );
        protocolMap.put( "https", "https" );
        protocolMap.put( "dav", "http" );
        protocolMap.put( "davs", "https" );
        protocolMap.put( "dav:http", "http" );
        protocolMap.put( "dav:https", "https" );
        protocolMap.put( "dav+http", "http" );
        protocolMap.put( "dav+https", "https" );
    }

    public static String getProtocol( String url )
    {
        // NOTE: Unlike Repository.getProtocol(), this handles "dav:http:" as well.

        int idx = url.indexOf( ":/" );
        if ( idx < 0 )
        {
            return "";
        }
        else
        {
            return url.substring( 0, idx );
        }
    }

    public static String normalizeProtocol( String protocol )
    {
        if ( protocolMap.containsKey( protocol ) )
        {
            return protocolMap.get( protocol );
        }
        else
        {
            return protocol;
        }
    }

    public static String buildUrl( String repositoryUrl, String resourceUrl )
        throws URISyntaxException
    {
        int idx = repositoryUrl.indexOf( "://" );
        if ( idx < 0 )
        {
            return repositoryUrl + resourceUrl;
        }

        String protocol = repositoryUrl.substring( 0, idx );
        protocol = normalizeProtocol( protocol );

        int authorityIdx = idx + 3;
        String authority;
        String path;
        idx = repositoryUrl.indexOf( '/', authorityIdx );
        if ( idx < 0 )
        {
            authority = repositoryUrl.substring( authorityIdx );
            path = "/";
        }
        else
        {
            authority = repositoryUrl.substring( authorityIdx, idx );
            path = repositoryUrl.substring( idx );
        }

        if ( !path.endsWith( "/" ) )
        {
            path += '/';
        }

        if ( resourceUrl.startsWith( "/" ) )
        {
            path += resourceUrl.substring( 1 );
        }
        else
        {
            path += resourceUrl;
        }

        return new URI( protocol, authority, path, null, null ).toASCIIString();
    }

}
