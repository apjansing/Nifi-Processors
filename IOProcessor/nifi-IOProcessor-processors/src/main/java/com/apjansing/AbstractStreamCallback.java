package com.apjansing;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.processor.io.StreamCallback;

public abstract class AbstractStreamCallback implements StreamCallback{

	@Override
	public void process(InputStream in, OutputStream out) throws IOException {
		IOUtils.copyLarge(run(in), out);
	}
	
	public abstract InputStream run(InputStream in);

}
