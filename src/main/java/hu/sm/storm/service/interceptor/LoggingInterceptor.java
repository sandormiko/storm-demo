package hu.sm.storm.service.interceptor;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingInterceptor implements MethodInterceptor {

	private static final Logger LOG = LoggerFactory.getLogger(LoggingInterceptor.class);

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		Object result = null;
		entering(invocation);
		try {
			result = invocation.proceed();
			return result;
		} finally {
			exiting(invocation);
		}
	}

	private void exiting(MethodInvocation invocation) {
		if (LOG.isDebugEnabled()) {
			String methodName = invocation.getMethod().getName();
			String clazz = invocation.getMethod().getDeclaringClass().getName();
			LOG.debug(String.format("Exiting %s.%s", clazz, methodName));

		}
	}

	private void entering(MethodInvocation invocation) {
		if (LOG.isDebugEnabled()) {
			String methodName = invocation.getMethod().getName();
			String clazz = invocation.getMethod().getDeclaringClass().getName();
			LOG.debug(String.format("Entering %s.%s ", clazz, methodName));
		}
	}
}
