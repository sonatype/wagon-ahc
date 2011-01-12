package org.sonatype.maven.wagon;

/*******************************************************************************
 * Copyright (c) 2010-2011 Sonatype, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

import java.io.IOException;
import java.io.PipedInputStream;

class PipedErrorInputStream
    extends PipedInputStream
{

    private volatile Throwable error;

    public PipedErrorInputStream()
    {
        buffer = new byte[1024 * 128];
    }

    public void setError( Throwable t )
    {
        if ( error == null )
        {
            error = t;
        }
    }

    private void checkError()
        throws IOException
    {
        if ( error != null )
        {
            throw (IOException) new IOException( error.getMessage() ).initCause( error );
        }
    }

    public synchronized int read()
        throws IOException
    {
        checkError();
        int b = super.read();
        checkError();
        return b;
    }

}
