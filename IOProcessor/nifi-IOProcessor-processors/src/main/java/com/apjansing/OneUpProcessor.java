package com.apjansing;

import java.io.InputStream;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

public class OneUpProcessor extends IOProcessor{

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}
		
		try{
			flowFile = session.write(flowFile, new AbstractStreamCallback() {

				@Override
				public InputStream run(InputStream in) {
					IOProcessorTest test = new IOProcessorTest(in);
					return test.getIn();
				}
			});
			session.transfer(flowFile, SUCCESS);
		}catch(Exception e){
			logger.error("TEST FAILED", e);
			session.transfer(flowFile, FAILURE);
		}
	}
	
}
