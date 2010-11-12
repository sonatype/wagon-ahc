package org.sonatype.maven.wagon;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;

import org.apache.maven.wagon.InputData;
import org.apache.maven.wagon.OutputData;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.StreamWagon;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.proxy.ProxyInfo;
import org.apache.maven.wagon.repository.Repository;
import org.apache.maven.wagon.resource.Resource;
import org.codehaus.plexus.util.StringUtils;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.AsyncHttpClientConfig.Builder;
import com.ning.http.client.FluentCaseInsensitiveStringsMap;
import com.ning.http.client.ProxyServer;
import com.ning.http.client.Realm;
import com.ning.http.client.Realm.RealmBuilder;
import com.ning.http.client.Response;

public class AhcWagon
    extends StreamWagon
{
    public static final int SC_OK = 200;
    public static final int SC_CREATED = 201;
    public static final int SC_ACCEPTED = 202;
    public static final int SC_NO_CONTENT = 204;
    public static final int SC_FORBIDDEN = 403;
    public static final int SC_NOT_FOUND = 404;
    public static final int SC_NOT_MODIFIED = 304;
    public static final int SC_UNAUTHORIZED = 401;
    public static final int SC_PROXY_AUTHENTICATION_REQUIRED = 407;
    public static final int SC_NULL = -1;    
    public static final String UTF8_ENCODING = "UTF-8";    
    public static final TimeZone GMT_TIME_ZONE = TimeZone.getTimeZone( "GMT" );
    
    private AsyncHttpClient client;
    private Builder asyncClientConfigBuilder;
    private RealmBuilder realmBuilder;    
    private BoundRequestBuilder requestBuilder;
    private FluentCaseInsensitiveStringsMap headers;    
    
    public AhcWagon()
    {
        client = new AsyncHttpClient();
        asyncClientConfigBuilder = new AsyncHttpClientConfig.Builder();
        headers = new FluentCaseInsensitiveStringsMap();                
    }
    
    public void openConnectionInternal()
    {
        repository.setUrl( getURL( repository ) );
                
        String username = null;
        String password = null;
        boolean realmRequired = false;
        
        if ( authenticationInfo != null )
        {
            username = authenticationInfo.getUserName();
            password = authenticationInfo.getPassword();
            
            realmBuilder = (new Realm.RealmBuilder())
                .setPrincipal( username )
                .setPassword( password )
                .setUsePreemptiveAuth( true )
                .setEnconding( UTF8_ENCODING );
            
            realmRequired = true;
        }

        ProxyInfo proxyInfo = getProxyInfo( getRepository().getProtocol(), getRepository().getHost() );
        
        if ( proxyInfo != null )
        {
            String proxyUsername = proxyInfo.getUserName();
            String proxyPassword = proxyInfo.getPassword();
            String proxyHost = proxyInfo.getHost();
            int proxyPort = proxyInfo.getPort();
            String proxyNtlmHost = proxyInfo.getNtlmHost();
            String proxyNtlmDomain = proxyInfo.getNtlmDomain();
            
            if ( proxyHost != null )
            {
                ProxyServer proxyServer = new ProxyServer( proxyHost, proxyPort );
                                
                if ( proxyUsername != null && proxyPassword != null )
                {
                    proxyServer = new ProxyServer( proxyHost, proxyPort, proxyUsername, proxyPassword );                    
                }
                else
                {
                    proxyServer = new ProxyServer( proxyHost, proxyPort );                    
                }

                if (  StringUtils.isNotEmpty( proxyNtlmDomain ) && StringUtils.isNotEmpty( proxyNtlmHost ) )
                {
                    realmBuilder
                        .setScheme( Realm.AuthScheme.NTLM)
                        .setDomain(proxyNtlmDomain)
                        .setPassword(proxyNtlmHost);
                }
                
                asyncClientConfigBuilder.setProxyServer( proxyServer );  
                realmRequired = true;
            }            
        }        
        
        if ( realmRequired )
        {
            asyncClientConfigBuilder.setRealm( realmBuilder.build() );
        }
    }

    public void closeConnection()
    {
        if( client != null )
        {
            client.close();
        }
    }

    public void put( File source, String resourceName )
        throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException
    {
        Resource resource = new Resource( resourceName );
        
        firePutInitiated( resource, source );
        
        resource.setContentLength( source.length() );
        
        resource.setLastModified( source.lastModified() );

        put( null, resource, source );
    }
    
    public void putFromStream( final InputStream stream, String destination, long contentLength, long lastModified )
        throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException
    {
        Resource resource = new Resource( destination );
        
        firePutInitiated( resource, null );
        
        resource.setContentLength( contentLength );
        
        resource.setLastModified( lastModified );
        
        put( stream, resource, null );
    }
    
    private void put( final InputStream stream, Resource resource, File source )
        throws TransferFailedException, AuthorizationException, ResourceDoesNotExistException
    {
        String url = getRepository().getUrl();
        String[] parts = StringUtils.split( resource.getName(), "/" );
        for ( int i = 0; i < parts.length; i++ )
        {
            // TODO: Fix encoding...
            // url += "/" + URLEncoder.encode( parts[i], System.getProperty("file.encoding") );
            url += "/" + URLEncoder.encode( parts[i] );
        }
        
        firePutStarted( resource, source );

        requestBuilder = client.preparePut( url );
        requestBuilder.setHeaders( headers );
        requestBuilder.setFollowRedirects( true );                
        requestBuilder.setBody( stream );
        requestBuilder.build();

        Response response;
        
        try
        {
            response = requestBuilder.execute().get();
        }
        catch ( InterruptedException e )
        {
            fireTransferError( resource, e, TransferEvent.REQUEST_PUT );

            throw new TransferFailedException( e.getMessage(), e );            
        }
        catch ( ExecutionException e )
        {
            fireTransferError( resource, e, TransferEvent.REQUEST_PUT );

            throw new TransferFailedException( e.getMessage(), e );            
        }
        catch ( IOException e )
        {
            fireTransferError( resource, e, TransferEvent.REQUEST_PUT );

            throw new TransferFailedException( e.getMessage(), e );            
        }

        int statusCode = response.getStatusCode();
            
        fireTransferDebug( url + " - Status code: " + statusCode );

        // Check that we didn't run out of retries.
        switch ( statusCode )
        {
            // Success Codes
            case SC_OK: // 200
            case SC_CREATED: // 201
            case SC_ACCEPTED: // 202
            case SC_NO_CONTENT: // 204
                break;

            case SC_NULL:
            {
                TransferFailedException e = new TransferFailedException( "Failed to transfer file: " + url );
                fireTransferError( resource, e, TransferEvent.REQUEST_PUT );
                throw e;
            }

            case SC_FORBIDDEN:
                fireSessionConnectionRefused();
                throw new AuthorizationException( "Access denied to: " + url );

            case SC_NOT_FOUND:
                throw new ResourceDoesNotExistException( "File: " + url + " does not exist" );
            
            default:
            {
                TransferFailedException e = new TransferFailedException( "Failed to transfer file: " + url + ". Return code is: " + statusCode );
                fireTransferError( resource, e, TransferEvent.REQUEST_PUT );
                throw e;
            }
        }

        firePutCompleted( resource, source );
    }
    
    public boolean resourceExists( String resourceName )
        throws TransferFailedException, AuthorizationException
    {
        String url = getRepository().getUrl() + "/" + resourceName;
        
        requestBuilder = client.prepareHead( url );
        requestBuilder.setHeaders( headers );
        requestBuilder.setFollowRedirects( true );        
        requestBuilder.build();

        Response response;
        
        try
        {
            response = requestBuilder.execute().get();
        }
        catch ( InterruptedException e )
        {
            throw new TransferFailedException( e.getMessage(), e );
        }
        catch ( ExecutionException e )
        {
            throw new TransferFailedException( e.getMessage(), e );
        }
        catch ( IOException e )
        {
            throw new TransferFailedException( e.getMessage(), e );
        }

        int statusCode = response.getStatusCode();        
        
        switch ( statusCode )
        {
            case SC_OK:
                return true;

            case SC_NOT_MODIFIED:
                return true;

            case SC_NULL:
                throw new TransferFailedException( "Failed to transfer file: " + url );

            case SC_FORBIDDEN:
                throw new AuthorizationException( "Access denied to: " + url );

            case SC_UNAUTHORIZED:
                throw new AuthorizationException( "Not authorized." );

            case SC_PROXY_AUTHENTICATION_REQUIRED:
                throw new AuthorizationException( "Not authorized by proxy." );

            case SC_NOT_FOUND:
                return false;

                // add more entries here
            default:
                throw new TransferFailedException( "Failed to transfer file: " + url + ". Return code is: "
                    + statusCode );
        }        
    }
    
    protected void setHeaders()
    {
        headers.add( "Cache-control", "no-cache" );
        headers.add( "Cache-store", "no-store" );
        headers.add( "Pragma", "no-cache" );
        headers.add( "Expires", "0" );
        headers.add( "Accept-Encoding", "gzip" );
    }

    /**
     * getUrl
     * Implementors can override this to remove unwanted parts of the url such as role-hints
     * @param repository
     * @return
     */
    protected String getURL( Repository repository )
    {
        return repository.getUrl();
    }

    protected AsyncHttpClient getClient()
    {
        return client;
    }

    public void fillInputData( InputData inputData )
        throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException
    {
        Resource resource = inputData.getResource();
        
        String url = getRepository().getUrl() + "/" + resource.getName();
        
        long timestamp = resource.getLastModified();
        if ( timestamp > 0 )
        {
            SimpleDateFormat fmt = new SimpleDateFormat( "EEE, dd-MMM-yy HH:mm:ss zzz", Locale.US );
            fmt.setTimeZone( GMT_TIME_ZONE );
            headers.add( "If-Modified-Since", fmt.format( new Date( timestamp ) ) );
            fireTransferDebug( "sending ==> header" + "(" + timestamp + ")" );
        }

        requestBuilder = client.prepareGet( url );        
        requestBuilder.setHeaders( headers );  
        requestBuilder.setFollowRedirects( true );        
        requestBuilder.build();

        Response response;
        
        try
        {
            response = requestBuilder.execute().get();
        }
        catch ( InterruptedException e )
        {
            throw new TransferFailedException( e.getMessage(), e );
        }
        catch ( ExecutionException e )
        {
            throw new TransferFailedException( e.getMessage(), e );
        }
        catch ( IOException e )
        {
            throw new TransferFailedException( e.getMessage(), e );
        }

        int statusCode = response.getStatusCode();        

        fireTransferDebug( url + " - Status code: " + statusCode );

        switch ( statusCode )
        {
            case SC_OK:
                break;

            case SC_NOT_MODIFIED:
                // return, leaving last modified set to original value so getIfNewer should return unmodified
                return;

            case SC_NULL:
            {
                TransferFailedException e = new TransferFailedException( "Failed to transfer file: " + url );
                fireTransferError( resource, e, TransferEvent.REQUEST_GET );
                throw e;
            }

            case SC_FORBIDDEN:
                fireSessionConnectionRefused();
                throw new AuthorizationException( "Access denied to: " + url );

            case SC_UNAUTHORIZED:
                fireSessionConnectionRefused();
                throw new AuthorizationException( "Not authorized." );

            case SC_PROXY_AUTHENTICATION_REQUIRED:
                fireSessionConnectionRefused();
                throw new AuthorizationException( "Not authorized by proxy." );

            case SC_NOT_FOUND:
                throw new ResourceDoesNotExistException( "File: " + url + " does not exist" );

            default:
            {
                cleanupGetTransfer( resource );
                TransferFailedException e =
                    new TransferFailedException( "Failed to transfer file: " + url + ". Return code is: "
                        + statusCode );
                fireTransferError( resource, e, TransferEvent.REQUEST_GET );
                throw e;
            }
        }

        InputStream is = null;

        String contentLengthHeader = response.getHeader( "Content-Length" );
        
        if ( contentLengthHeader != null )
        {
            try
            {
                long contentLength = Integer.valueOf( contentLengthHeader ).intValue();

                resource.setContentLength( contentLength );
            }
            catch ( NumberFormatException e )
            {
                fireTransferDebug( "error parsing content length header '" + contentLengthHeader + "' "
                    + e );
            }
        }

        String lastModifiedHeader = response.getHeader( "Last-Modified" );

        long lastModified = 0;

        if ( lastModifiedHeader != null )
        {
            try
            {
                lastModified = DateUtil.parseDate( lastModifiedHeader ).getTime();

                resource.setLastModified( lastModified );
            }
            catch ( DateParseException e )
            {
                fireTransferDebug( "Unable to parse last modified header" );
            }

            fireTransferDebug( "last-modified = " + lastModifiedHeader + " (" + lastModified + ")" );
        }

        String contentEncoding = response.getHeader( "Content-Encoding" );
        boolean isGZipped = contentEncoding == null ? false : "gzip".equalsIgnoreCase( contentEncoding );

        try
        {
            is = response.getResponseBodyAsStream();
            
            if ( isGZipped )
            {
                is = new GZIPInputStream( is );
            }
        }
        catch ( IOException e )
        {
            fireTransferError( resource, e, TransferEvent.REQUEST_GET );

            String msg =
                
                "Error occurred while retrieving from remote repository:" + getRepository() + ": " + e.getMessage();
            
            throw new TransferFailedException( msg, e );
        }
        
        inputData.setInputStream( is );
    }

    public void fillOutputData( OutputData outputData )
        throws TransferFailedException
    {
        throw new IllegalStateException( "Should not be using the streaming wagon for HTTP PUT" );        
    }

    public void setHttpHeaders( Properties properties )
    {
        for( Object key : properties.keySet() )
        {
            headers.add(  (String)key, properties.getProperty( (String)key ) );
        }
    }   
}
