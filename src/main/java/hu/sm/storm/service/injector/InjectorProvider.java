package hu.sm.storm.service.injector;

import com.google.inject.Guice;
import com.google.inject.Injector;

import hu.sm.storm.service.module.ServiceModule;

public class InjectorProvider {

	private static Injector injector;

	public static Injector getInjector() {
		if (injector == null) {
			synchronized (Injector.class) {
				if (injector == null) {
					injector = Guice.createInjector(new ServiceModule());
				}
			}
		}
		return injector;
	}
}
