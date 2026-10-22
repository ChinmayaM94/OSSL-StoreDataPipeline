package com.safeway.ossl.storepipeline.controller;

import com.safeway.ossl.storepipeline.dto.ImportResult;
import com.safeway.ossl.storepipeline.exception.GlobalExceptionHandler;
import com.safeway.ossl.storepipeline.service.CsvStoreImportService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(StoreImportController.class)
@Import(GlobalExceptionHandler.class)
class StoreImportControllerValidationTest {

    private static final String URL = com.safeway.ossl.storepipeline.constants.OSSLServiceEndPoints.OSSL_SERVICE_API_PREFIX
            + com.safeway.ossl.storepipeline.constants.OSSLServiceEndPoints.CSV_INGESTION_URL;

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private CsvStoreImportService service;

    @Test
    void validCsv_isAccepted() throws Exception {
        when(service.importCsv(any())).thenReturn(new ImportResult(1, 1, 0, List.of()));
        mockMvc.perform(multipart(URL).file(new MockMultipartFile("file", "stores.csv", "text/csv", "a,b".getBytes())))
                .andExpect(status().isOk());
    }

    @Test
    void emptyFile_isRejectedWith400() throws Exception {
        mockMvc.perform(multipart(URL).file(new MockMultipartFile("file", "stores.csv", "text/csv", new byte[0])))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("missing or empty")));
        verify(service, never()).importCsv(any());
    }

    @Test
    void nonCsvFile_isRejectedWith400() throws Exception {
        mockMvc.perform(multipart(URL).file(new MockMultipartFile("file", "stores.pdf", "application/pdf", "x".getBytes())))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("does not appear to be a CSV")));
        verify(service, never()).importCsv(any());
    }
}
