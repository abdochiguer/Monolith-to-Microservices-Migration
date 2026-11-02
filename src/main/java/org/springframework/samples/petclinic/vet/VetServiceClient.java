/*
 * Copyright 2012-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.samples.petclinic.vet;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Component
class VetServiceClient {

	private final RestClient restClient;

	VetServiceClient(@Value("${petclinic.services.vet.base-url:}") String baseUrl,
			@Value("${petclinic.services.vet.connect-timeout:2s}") Duration connectTimeout,
			@Value("${petclinic.services.vet.read-timeout:15s}") Duration readTimeout) {
		if (StringUtils.hasText(baseUrl)) {
			SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
			requestFactory.setConnectTimeout(connectTimeout);
			requestFactory.setReadTimeout(readTimeout);
			this.restClient = RestClient.builder().baseUrl(baseUrl).requestFactory(requestFactory).build();
		}
		else {
			this.restClient = null;
		}
	}

	boolean isConfigured() {
		return this.restClient != null;
	}

	Vets findAll() {
		if (this.restClient == null) {
			throw new IllegalStateException("Vet service URL is not configured");
		}
		Vets vets = this.restClient.get().uri("/vets").retrieve().body(Vets.class);
		if (vets == null) {
			throw new RestClientException("Vet service returned an empty response");
		}
		return vets;
	}

}
