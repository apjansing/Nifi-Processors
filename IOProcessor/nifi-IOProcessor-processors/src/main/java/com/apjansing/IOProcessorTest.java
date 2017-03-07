package com.apjansing;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

public class IOProcessorTest {

	private InputStream in = null;
	private byte[] sample = null;
	
	public IOProcessorTest(InputStream in){
		setIn(in);
		run();
		setIn(new ByteArrayInputStream(getSample()));
	}

	private void run() {
		try {
			setSample(IOUtils.toByteArray(getIn()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(int i = 0; i < sample.length; i++)
			sample[i] = (byte) (sample[i] + 1);
	}

	/**
	 * @return the in
	 */
	public InputStream getIn() {
		return in;
	}

	/**
	 * @param in the in to set
	 */
	public void setIn(InputStream in) {
		this.in = in;
	}
	
	/**
	 * @return is sample
	 */
	public byte[] getSample(){
		return sample;
	}
	
	/**
	 * @param b the sample to set
	 */
	public void setSample(byte[] b) {
		this.sample = b;
	}
}
