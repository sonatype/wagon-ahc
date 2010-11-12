package org.sonatype.maven.wagon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;

import org.apache.maven.wagon.ConnectionException;
import org.apache.maven.wagon.InputData;
import org.apache.maven.wagon.OutputData;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.StreamWagon;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.proxy.ProxyInfo;
import org.apache.maven.wagon.resource.Resource;
import org.codehaus.plexus.util.StringUtils;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClient.BoundRequestBuilder;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.ProxyServer;
import com.ning.http.client.Realm;
import com.ning.http.client.Realm.RealmBuilder;
import com.ning.http.client.Response;

public class AhcWagon
    extends StreamWagon
{
    /**
     * @plexus.configuration default="false"
     */
    private boolean useCache;

    /**
     * @plexus.configuration default="10"
     */
    private int maxRedirections = 10;

    /**
     * @plexus.configuration
     */
    private Properties httpHeaders;

    private AsyncHttpClient httpClient;

    @Override
    protected void openConnectionInternal()
        throws ConnectionException, AuthenticationException
    {
        String protocol = UrlUtils.getProtocol( getRepository().getUrl() );
        protocol = UrlUtils.normalizeProtocol( protocol );
        AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder();
        RealmBuilder realmBuilder = null;
        
        if ( authenticationInfo != null )
        {
            String username = authenticationInfo.getUserName();
            String password = authenticationInfo.getPassword();
            
            realmBuilder = (new Realm.RealmBuilder())
                .setPrincipal( username )
                .setPassword( password )
                .setUsePreemptiveAuth( true )
                .setEnconding( "UTF-8" );      
            
            builder.setRealm( realmBuilder.build() );
        }        
        
        ProxyInfo proxyInfo = getProxyInfo( protocol, getRepository().getHost() );
        if ( proxyInfo == null && "https".equalsIgnoreCase( protocol ) )
        {
            proxyInfo = getProxyInfo( "http", getRepository().getHost() );
        }
        
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
                    if ( realmBuilder == null )
                    {
                        realmBuilder = (new Realm.RealmBuilder());
                    }
                    
                    realmBuilder
                        .setScheme( Realm.AuthScheme.NTLM)
                        .setDomain(proxyNtlmDomain)
                        .setPassword(proxyNtlmHost);
                }
                
                builder.setProxyServer( proxyServer );  
            }                    
        }
        
        builder.setConnectionTimeoutInMs( getTimeout() );
        builder.setRequestTimeoutInMs( getTimeout() );
        builder.setFollowRedirects( maxRedirections > 0 );
        builder.setMaximumNumberOfRedirects( maxRedirections );
        builder.setUserAgent( "Apache-Maven" );
        
        if ( httpHeaders != null && httpHeaders.getProperty( "User-Agent" ) != null )
        {
            builder.setUserAgent( httpHeaders.getProperty( "User-Agent" ) );
        }
                
        httpClient = new AsyncHttpClient( builder.build() );
    }

    @Override
    public void closeConnection()
        throws ConnectionException
    {
        if ( httpClient != null )
        {
            httpClient.close();
            httpClient = null;
        }
    }

    public void setHttpHeaders( Properties properties )
    {
        if ( properties != null )
        {
            httpHeaders = new Properties();
            httpHeaders.putAll( properties );
        }
        else
        {
            httpHeaders = null;
        }
    }

    private void addHeaders( BoundRequestBuilder builder )
    {
        builder.addHeader( "Accept-Encoding", "gzip" );

        if ( !useCache )
        {
            builder.addHeader( "Pragma", "no-cache" );
            builder.addHeader( "Cache-Control", "no-cache, no-store" );
        }

        if ( httpHeaders != null )
        {
            for ( Object key : httpHeaders.keySet() )
            {
                builder.addHeader( key.toString(), httpHeaders.getProperty( key.toString() ) );
            }
        }
    }

    @Override
    public boolean resourceExists( String resourceName )
        throws TransferFailedException, AuthorizationException
    {
        try
        {
            String url = UrlUtils.buildUrl( getRepository().getUrl(), resourceName );

            BoundRequestBuilder builder = httpClient.prepareHead( url );
            addHeaders( builder );
            Response response = builder.execute().get();
            
            int statusCode = response.getStatusCode();
            switch ( statusCode )
            {
                case HttpURLConnection.HTTP_OK:
                    return true;
                    
                case HttpURLConnection.HTTP_NOT_FOUND:
                    return false;
                    
                case HttpURLConnection.HTTP_UNAUTHORIZED:                    
                case HttpURLConnection.HTTP_FORBIDDEN:
                    throw new AuthorizationException( "Access denied to: " + url + " (" + statusCode + ")" );
                
                default:
                    throw new TransferFailedException( "Failed to check for existence of resource " + url + " ("
                        + statusCode + ")" );
            }
        }
        catch ( URISyntaxException e )
        {
            return false;
        }
        catch ( IOException e )
        {
            throw new TransferFailedException( "Error transferring file: " + e.getMessage(), e );
        }
        catch ( InterruptedException e )
        {
            throw new TransferFailedException( "Transfer was aborted by client", e );
        }
        catch ( ExecutionException e )
        {
            throw new TransferFailedException( "Transfer was aborted by client", e );
        }
    }

    @Override
    public void fillInputData( InputData inputData )
        throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException
    {
        Resource resource = inputData.getResource();

        try
        {
            String url = UrlUtils.buildUrl( getRepository().getUrl(), resource.getName() );

            BoundRequestBuilder builder = httpClient.prepareGet( url );
            builder.setFollowRedirects( true );
            addHeaders( builder );
            Response response = builder.execute().get();
                        
            int statusCode = response.getStatusCode();
            switch ( statusCode )
            {
                case HttpURLConnection.HTTP_OK:
                case HttpURLConnection.HTTP_NOT_MODIFIED:
                    break;
                    
                case HttpURLConnection.HTTP_UNAUTHORIZED:
                case HttpURLConnection.HTTP_FORBIDDEN:
                    throw new AuthorizationException( "Access denied to: " + url + " (" + statusCode + ")" );
                
                case HttpURLConnection.HTTP_NOT_FOUND:
                    throw new ResourceDoesNotExistException( "Unable to locate resource in repository" );
                
                default:
                    throw new TransferFailedException( "Error transferring file, server returned status code " + statusCode );
            }

            String contentEncoding = response.getHeader( "Content-Encoding" );
            boolean isGZipped = contentEncoding == null ? false : "gzip".equalsIgnoreCase( contentEncoding );

            if ( isGZipped )
            {
                inputData.setInputStream( new GZIPInputStream( response.getResponseBodyAsStream() ) );
            }
            else
            {
                inputData.setInputStream( response.getResponseBodyAsStream() );
            }
            
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
        }
        catch ( URISyntaxException e )
        {
            throw new ResourceDoesNotExistException( "Invalid repository URL", e );
        }
        catch ( IOException e )
        {
            throw new TransferFailedException( "Error transferring file: " + e.getMessage(), e );
        }
        catch ( InterruptedException e )
        {
            throw new TransferFailedException( "Transfer was aborted by client", e );
        }
        catch ( ExecutionException e )
        {
            throw new TransferFailedException( "Transfer was aborted by client", e );
        }
    }

    @Override
    public void fillOutputData( OutputData outputData )
        throws TransferFailedException
    {
        Resource resource = outputData.getResource();

        try
        {
            String url = UrlUtils.buildUrl( getRepository().getUrl(), resource.getName() );

            BoundRequestBuilder builder = httpClient.preparePut( url );
            addHeaders( builder );
                                    
            PutOutputStream pos = new PutOutputStream( builder, url, resource.getContentLength() );

            outputData.setOutputStream( pos );
        }
        catch ( URISyntaxException e )
        {
            throw new TransferFailedException( "Invalid repository URL", e );
        }
    }

    @Override
    protected void finishPutTransfer( Resource resource, InputStream input, OutputStream output )
        throws TransferFailedException, AuthorizationException, ResourceDoesNotExistException
    {        
        PutOutputStream pos = (PutOutputStream) output;
        
        try
        {
            Response response = pos.send();
            int statusCode = response.getStatusCode();

            switch ( statusCode )
            {
                case HttpURLConnection.HTTP_OK:
                case HttpURLConnection.HTTP_CREATED:
                case HttpURLConnection.HTTP_ACCEPTED:
                case HttpURLConnection.HTTP_NO_CONTENT:
                    break;

                case HttpURLConnection.HTTP_UNAUTHORIZED:
                case HttpURLConnection.HTTP_FORBIDDEN:
                    throw new AuthorizationException( "Access denied to: " + pos.getUrl() + " (" + statusCode + ")" );

                case HttpURLConnection.HTTP_NOT_FOUND:
                    throw new ResourceDoesNotExistException( "File: " + pos.getUrl() + " does not exist" );

                default:
                    throw new TransferFailedException( "Failed to transfer file: " + pos.getUrl()
                        + ". Return code is: " + statusCode );
            }
        }
        catch ( IOException e )
        {
            fireTransferError( resource, e, TransferEvent.REQUEST_PUT );

            throw new TransferFailedException( "Error transferring file", e );
        }
    }
}
