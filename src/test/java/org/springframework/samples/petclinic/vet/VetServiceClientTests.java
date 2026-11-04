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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VetServiceClientTests {

	@Test
	void mapsVetsReturnedByTheVetService() throws IOException {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/vets", exchange -> {
			byte[] response = """
					{"vetList":[{"id":2,"firstName":"Helen","lastName":"Leary","specialties":[]}]}
					""".strip().getBytes(java.nio.charset.StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/json");
			exchange.sendResponseHeaders(200, response.length);
			try (OutputStream responseBody = exchange.getResponseBody()) {
				responseBody.write(response);
			}
		});
		server.start();
		try {
			String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
			Vets vets = new VetServiceClient(baseUrl, Duration.ofSeconds(1), Duration.ofSeconds(1)).findAll();

			assertThat(vets.getVetList()).singleElement().satisfies(vet -> {
				assertThat(vet.getId()).isEqualTo(2);
				assertThat(vet.getFirstName()).isEqualTo("Helen");
				assertThat(vet.getLastName()).isEqualTo("Leary");
			});
		}
		finally {
			server.stop(0);
		}
	}

}
