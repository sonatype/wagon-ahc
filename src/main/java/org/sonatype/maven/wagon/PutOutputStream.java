package org.sonatype.maven.wagon;

/*******************************************************************************
 * Copyright (c) 2010-2011 Sonatype, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;

import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;
import com.ning.http.client.Request;
import com.ning.http.client.Response;

public class PutOutputStream
    extends ByteArrayOutputStream
{

    private final BoundRequestBuilder builder;

    private final String url;

    private Response response;

    public PutOutputStream( BoundRequestBuilder builder, String url, long size )
    {
        super( Math.max( (int) size, 1024 * 4 ) );
        this.builder = builder;
        this.url = url;
    }

    public String getUrl()
    {
        return url;
    }

    public Response send()
        throws IOException
    {
        if ( response != null )
        {
            throw new IllegalStateException( "Request already sent" );
        }

        builder.setBody( new Request.EntityWriter()
        {
            public void writeEntity( OutputStream out )
                throws IOException
            {
                writeTo( out );
            }
        } );        
                
        try
        {
            response = builder.execute().get();
            return response;
        }
        catch ( InterruptedException e )
        {
            throw (IOException) new IOException( e.getMessage() ).initCause( e );
        }
        catch ( ExecutionException e )
        {
            throw (IOException) new IOException( e.getCause().getMessage() ).initCause( e.getCause() );
        }
    }

}
