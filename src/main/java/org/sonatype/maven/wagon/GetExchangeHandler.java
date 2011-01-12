package org.sonatype.maven.wagon;

/*******************************************************************************
 * Copyright (c) 2010-2011 Sonatype, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

import java.io.IOException;

import com.ning.http.client.AsyncHandler;
import com.ning.http.client.FluentCaseInsensitiveStringsMap;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;

class GetExchangeHandler
    implements AsyncHandler<String>
{

    private GetExchange exchange;

    public GetExchangeHandler( GetExchange exchange )
    {
        this.exchange = exchange;
    }

    public AsyncHandler.STATE onStatusReceived( HttpResponseStatus responseStatus )
        throws Exception
    {
        exchange.setStatusCode( responseStatus.getStatusCode() );

        return STATE.CONTINUE;
    }

    public AsyncHandler.STATE onHeadersReceived( HttpResponseHeaders headers )
        throws Exception
    {
        FluentCaseInsensitiveStringsMap h = headers.getHeaders();

        exchange.setLastModified( h.getFirstValue( "Last-Modified" ) );
        exchange.setContentLength( h.getFirstValue( "Content-Length" ) );

        exchange.start();

        return STATE.CONTINUE;
    }

    public AsyncHandler.STATE onBodyPartReceived( HttpResponseBodyPart bodyPart )
        throws Exception
    {
        bodyPart.writeTo( exchange.getOutputStream() );

        return STATE.CONTINUE;
    }

    public String onCompleted()
        throws Exception
    {
        if ( exchange != null )
        {
            exchange.getOutputStream().close();
            exchange.start();
        }

        return "";
    }

    public void onThrowable( Throwable t )
    {
        if ( exchange != null )
        {
            exchange.fail( t );
            exchange.start();
            try
            {
                exchange.getOutputStream().close();
            }
            catch ( IOException e )
            {
                // ignored
            }
        }
    }

}
