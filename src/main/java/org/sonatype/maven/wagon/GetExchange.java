package org.sonatype.maven.wagon;

/*******************************************************************************
 * Copyright (c) 2010-2011 Sonatype, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CountDownLatch;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.util.DateUtil;


class GetExchange
{

    private final AsyncHttpClient httpClient;

    private final CountDownLatch latch;

    private int statusCode;

    private long contentLength = -1;

    private long lastModified;

    private PipedErrorInputStream inputStream;

    private PipedOutputStream outputStream;

    private Throwable error;

    public GetExchange( AsyncHttpClient httpClient )
        throws IOException
    {
        this.httpClient = httpClient;
        this.latch = new CountDownLatch( 1 );
        this.inputStream = new PipedErrorInputStream();
        this.outputStream = new PipedOutputStream( inputStream );
    }

    public AsyncHttpClient getHttpClient()
    {
        return httpClient;
    }

    public void await()
        throws InterruptedException
    {
        latch.await();
    }

    public void start()
    {
        latch.countDown();
    }

    public int getStatusCode()
    {
        return statusCode;
    }

    public void setStatusCode( int statusCode )
    {
        this.statusCode = statusCode;
    }

    public long getLastModified()
    {
        return lastModified;
    }

    public void setLastModified( long lastModified )
    {
        this.lastModified = lastModified;
    }

    public void setLastModified( String lastModified )
    {
        if ( lastModified != null && lastModified.length() > 0 )
        {
            try
            {
                this.lastModified = DateUtil.parseDate( lastModified ).getTime();
            }
            catch ( DateUtil.DateParseException e )
            {
                this.lastModified = -1;
            }
        }
    }

    public void setContentLength( int contentLength )
    {
        this.contentLength = contentLength;
    }

    public void setContentLength( String contentLength )
    {
        if ( contentLength != null && contentLength.length() > 0 )
        {
            try
            {
                this.contentLength = Long.parseLong( contentLength );
            }
            catch ( NumberFormatException e )
            {
                this.contentLength = -1;
            }
        }
        else
        {
            this.contentLength = -1;
        }
    }

    public long getContentLength()
    {
        return contentLength;
    }

    public InputStream getInputStream()
    {
        return inputStream;
    }

    public OutputStream getOutputStream()
    {
        return outputStream;
    }

    public void fail( Throwable error )
    {
        this.error = error;
        inputStream.setError( error );
    }

    public Throwable getError()
    {
        return error;
    }

}
