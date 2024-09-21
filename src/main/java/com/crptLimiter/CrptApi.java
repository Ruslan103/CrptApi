package com.crptLimiter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class CrptApi {

    private final Semaphore semaphore;
    private final ScheduledExecutorService scheduler;
    private final AtomicInteger requestCounter;
    private final int requestLimit;
    private final TimeUnit timeUnit;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.timeUnit = timeUnit;
        this.requestLimit = requestLimit;
        this.semaphore = new Semaphore(requestLimit);
        this.requestCounter = new AtomicInteger(0);
        this.scheduler = Executors.newScheduledThreadPool(1);

        scheduler.scheduleAtFixedRate(new ResetRequestCounterTask(), 0, 1, timeUnit);
    }

    public void createDocument(Document document, String signature) {
        DocumentConverter documentConverter = new JsonDocumentConverter();
        DocumentSender documentSender = new HttpDocumentSender();
        semaphoreAcquire();
        String jsonDocument = documentConverter.convertDocument(document);
        documentSender.sendDocument(jsonDocument, signature);
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    private void semaphoreAcquire() {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private class ResetRequestCounterTask implements Runnable {
        @Override
        public void run() {
            semaphore.release(requestLimit - requestCounter.getAndSet(0));
        }
    }

    private class HttpDocumentSender implements DocumentSender {
        public void sendDocument(String jsonDocument, String signature) {
            HttpURLConnection connection = createConnection(signature);
            sendJsonDocument(connection, jsonDocument);
            handleResponse(connection);
            finalizeRequest(connection);
        }

        private HttpURLConnection createConnection(String signature) {
            try {
                URL url = new URL("https://ismp.crpt.ru/api/v3/lk/documents/create");
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Type", "application/json");
                connection.setRequestProperty("Signature", signature);
                connection.setDoOutput(true);
                return connection;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void sendJsonDocument(HttpURLConnection connection, String jsonDocument) {
            try (OutputStream os = connection.getOutputStream()) {
                os.write(jsonDocument.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void handleResponse(HttpURLConnection connection) {
            int responseCode = 0;
            try {
                responseCode = connection.getResponseCode();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (responseCode != HttpURLConnection.HTTP_OK) {
                try (InputStream errorStream = connection.getErrorStream()) {
                    String errorMessage = new String(errorStream.readAllBytes(), StandardCharsets.UTF_8);
                    throw new IOException("HTTP response code: " + responseCode + ", Error: " + errorMessage);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private void finalizeRequest(HttpURLConnection connection) {
            requestCounter.incrementAndGet();
            semaphore.release();
            if (connection != null) {
                connection.disconnect();
            }
        }

    }

    private class JsonDocumentConverter implements DocumentConverter {
        @Override
        public String convertDocument(Document document) {
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonDocument;
            try {
                jsonDocument = objectMapper.writeValueAsString(document);
            } catch (JsonProcessingException e) {
                log.info(e.getMessage());
                throw new RuntimeException(e);
            }
            return jsonDocument;
        }
    }

    public interface DocumentConverter {
        String convertDocument(Document document);
    }

    public interface DocumentSender {
        void sendDocument(String jsonDocument, String signature);
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    private class Document {
        @JsonProperty("description")
        private Description description;

        @JsonProperty("doc_id")
        private String docId;

        @JsonProperty("doc_status")
        private String docStatus;

        @JsonProperty("doc_type")
        private String docType;

        @JsonProperty("importRequest")
        private boolean importRequest;

        @JsonProperty("owner_inn")
        private String ownerInn;

        @JsonProperty("participant_inn")
        private String participantInn;

        @JsonProperty("producer_inn")
        private String producerInn;

        @JsonProperty("production_date")
        private String productionDate;

        @JsonProperty("production_type")
        private String productionType;

        @JsonProperty("products")
        private List<Product> products;

        @JsonProperty("reg_date")
        private String regDate;

        @JsonProperty("reg_number")
        private String regNumber;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Product {
        @JsonProperty("certificate_document")
        private String certificateDocument;

        @JsonProperty("certificate_document_date")
        private String certificateDocumentDate;

        @JsonProperty("certificate_document_number")
        private String certificateDocumentNumber;

        @JsonProperty("owner_inn")
        private String ownerInn;

        @JsonProperty("producer_inn")
        private String producerInn;

        @JsonProperty("production_date")
        private String productionDate;

        @JsonProperty("tnved_code")
        private String tnvedCode;

        @JsonProperty("uit_code")
        private String uitCode;

        @JsonProperty("uitu_code")
        private String uituCode;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    private class Description {
        @JsonProperty("participantInn")
        private String participantInn;

    }
}

