/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cloud.stream.app.loggregator.source;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.cloudfoundry.client.lib.ApplicationLogListener;
import org.cloudfoundry.client.lib.CloudFoundryClient;
import org.cloudfoundry.client.lib.domain.ApplicationLog;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Test the Spring Cloud Dataflow CloudFoundry Loggregator source
 *
 * @author Josh Long
 * @author Gary Russell
 * @author Chris Schaefer
 */
@RunWith(SpringJUnit4ClassRunner.class)
@EnableConfigurationProperties(LoggregatorProperties.class)
@SpringBootTest({"loggregator.applicationName=foo", "loggregator.cloudFoundryUser=bar",
		"loggregator.cloudFoundryPassword=baz", "loggregator.cloudFoundryApi=qux"})
@DirtiesContext
public class LoggregatorSourceTests {
	@Autowired
	private Source channels;

	@Autowired
	private MessageCollector messageCollector;

	@Test
	public void testLogReceipt() throws Exception {
		Message<?> received = this.messageCollector.forChannel(this.channels.output()).poll(10, TimeUnit.SECONDS);
		assertNotNull(received);
		assertThat(received.getPayload(), is(equalTo("hello")));
		assertThat(received.getHeaders()
						.get(LoggregatorMessageSource.LoggregatorHeaders.APPLICATION_ID.asHeader()),
				is(equalTo("foo")));
		assertThat(received.getHeaders()
						.get(LoggregatorMessageSource.LoggregatorHeaders.TIMESTAMP.asHeader()),
				is(equalTo(new Date(1))));
		assertThat(received.getHeaders()
						.get(LoggregatorMessageSource.LoggregatorHeaders.MESSAGE_TYPE.asHeader()),
				is(equalTo(ApplicationLog.MessageType.STDOUT)));
		assertThat(received.getHeaders()
						.get(LoggregatorMessageSource.LoggregatorHeaders.SOURCE_NAME.asHeader()),
				is(equalTo("srcN")));
		assertThat(received.getHeaders()
						.get(LoggregatorMessageSource.LoggregatorHeaders.SOURCE_ID.asHeader()),
				is(equalTo("srcID")));
	}

	@SpringBootApplication
	public static class LoggregatorSourceApplication {
		@Bean
		public CloudFoundryClient cloudFoundryClient() {
			CloudFoundryClient mockClient = mock(CloudFoundryClient.class);
			doAnswer(invocation -> {
				ApplicationLogListener listener = invocation.getArgument(1);
				listener.onMessage(new ApplicationLog("foo", "hello", new Date(1),
						ApplicationLog.MessageType.STDOUT, "srcN", "srcID"));
				listener.onComplete();
				return null;
			}).when(mockClient).streamLogs(eq("foo"), any(ApplicationLogListener.class));

			return mockClient;
		}
	}
}
