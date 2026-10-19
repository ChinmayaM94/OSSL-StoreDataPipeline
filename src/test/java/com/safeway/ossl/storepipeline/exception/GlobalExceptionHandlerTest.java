package com.safeway.ossl.storepipeline.exception;

import com.mongodb.MongoTimeoutException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    void mongoConnectionFailure_returns503WithoutLeakingDriverDetails() {
        String driverMessage = "Timed out while waiting for a server that matches ReadPreferenceServerSelector";
        DataAccessResourceFailureException ex = new DataAccessResourceFailureException(
                driverMessage, new MongoTimeoutException(driverMessage));

        ResponseEntity<Map<String, Object>> response = handler.handleDataStoreUnavailable(ex);

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        String message = (String) response.getBody().get("message");
        assertEquals("MongoDB is currently unavailable. Please try again shortly.", message);
        assertFalse(message.contains("ReadPreferenceServerSelector"));
    }

    @Test
    void unknownDataStoreFailure_usesGenericName() {
        ResponseEntity<Map<String, Object>> response =
                handler.handleDataStoreUnavailable(new DataAccessResourceFailureException("boom"));

        assertEquals(503, response.getBody().get("status"));
        assertEquals("A data store is currently unavailable. Please try again shortly.", response.getBody().get("message"));
    }
}
