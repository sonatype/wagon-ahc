package org.sonatype.maven.wagon;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.ning.http.client.Response;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;

public class PutOutputStream
    extends ByteArrayOutputStream
{

    private final BoundRequestBuilder builder;

    private final String url;

    private Response response;

    public PutOutputStream( BoundRequestBuilder builder, String url, long size )
    {
        super( (int) size );
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

        if ( buf.length == count )
        {
            builder.setBody( buf );
        }
        else
        {
            builder.setBody( toByteArray() );
        }
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
