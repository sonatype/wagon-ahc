package org.sonatype.maven.wagon;

/*******************************************************************************
 * Copyright (c) 2010-2011 Sonatype, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.resource.Resource;

import com.ning.http.client.generators.FileBodyGenerator;
import com.ning.http.client.RandomAccessBody;

class ProgressingFileBodyGenerator
    extends FileBodyGenerator
{

    private final File file;

    private final AhcWagon wagon;

    private final TransferEvent event;

    private byte[] bytes = new byte[1024 * 16];

    public ProgressingFileBodyGenerator( File file, Resource resource, AhcWagon wagon )
    {
        super( file );
        this.file = file;
        this.wagon = wagon;

        event = new TransferEvent( wagon, resource, TransferEvent.TRANSFER_PROGRESS, TransferEvent.REQUEST_PUT );
        event.setTimestamp( System.currentTimeMillis() );
    }

    void fireTransferProgressed( ByteBuffer buffer )
    {
        int count = buffer.remaining();
        if ( count > bytes.length )
        {
            bytes = new byte[count];
        }
        buffer.get( bytes, 0, count );

        wagon.fireTransferProgressed( event, count, bytes );
    }

    @Override
    public RandomAccessBody createBody()
        throws IOException
    {
        wagon.firePutStarted( file, event.getResource() );

        return new ProgressingBody( super.createBody() );
    }

    final class ProgressingBody
        implements RandomAccessBody
    {

        final RandomAccessBody delegate;

        private ProgressingWritableByteChannel channel;

        public ProgressingBody( RandomAccessBody delegate )
        {
            this.delegate = delegate;
        }

        public long getContentLength()
        {
            return delegate.getContentLength();
        }

        public long read( ByteBuffer buffer )
            throws IOException
        {
            ByteBuffer eventBuffer = buffer.slice();
            long read = delegate.read( buffer );
            if ( read > 0 )
            {
                eventBuffer.limit( (int) read );
                fireTransferProgressed( eventBuffer );
            }
            return read;
        }

        public long transferTo( long position, long count, WritableByteChannel target )
            throws IOException
        {
            ProgressingWritableByteChannel dst = channel;
            if ( dst == null || dst.delegate != target )
            {
                channel = dst = new ProgressingWritableByteChannel( target );
            }
            return delegate.transferTo( position, Math.min( count, 1024 * 16 ), dst );
        }

        public void close()
            throws IOException
        {
            delegate.close();
        }

    }

    final class ProgressingWritableByteChannel
        implements WritableByteChannel
    {

        final WritableByteChannel delegate;

        public ProgressingWritableByteChannel( WritableByteChannel delegate )
        {
            this.delegate = delegate;
        }

        public boolean isOpen()
        {
            return delegate.isOpen();
        }

        public void close()
            throws IOException
        {
            delegate.close();
        }

        public int write( ByteBuffer src )
            throws IOException
        {
            ByteBuffer eventBuffer = src.slice();
            int written = delegate.write( src );
            if ( written > 0 )
            {
                eventBuffer.limit( written );
                fireTransferProgressed( eventBuffer );
            }
            return written;
        }

    }

}
