package hu.sm.storm.service.module;

import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;

import hu.sm.storm.service.api.MessageExtractorService;
import hu.sm.storm.service.impl.MessageExtractorServiceImpl;
import hu.sm.storm.service.interceptor.LoggingInterceptor;
import hu.sm.storm.service.interceptor.qualifier.EnteringExitingLogged;
import hu.sm.storm.service.serialization.api.KafkaTestDeserializer;
import hu.sm.storm.service.serialization.api.KafkaTestSerializer;
import hu.sm.storm.service.serialization.impl.KafkaTestDeserializerImpl;
import hu.sm.storm.service.serialization.impl.KafkaTestSerializerImpl;

public class ServiceModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(KafkaTestSerializer.class).to(KafkaTestSerializerImpl.class);
		bind(KafkaTestDeserializer.class).to(KafkaTestDeserializerImpl.class);
		bind(MessageExtractorService.class).to(MessageExtractorServiceImpl.class);
		bindInterceptor(Matchers.annotatedWith(EnteringExitingLogged.class), Matchers.any(), new LoggingInterceptor());
	}
}
