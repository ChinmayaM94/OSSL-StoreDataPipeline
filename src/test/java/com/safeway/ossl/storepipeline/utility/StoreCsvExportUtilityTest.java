package com.safeway.ossl.storepipeline.utility;

import com.safeway.ossl.storepipeline.repository.GroceryStoreMongoRepository;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("local")
class StoreCsvExportUtilityTest {

    @Autowired
    private GroceryStoreMongoRepository storeRepository;

    @Test
    @Disabled("Run manually - hits real MongoDB and writes a file to disk")
    void testExportStoresToCsv() throws IOException {
        StoreCsvExportUtility exportUtility = new StoreCsvExportUtility(storeRepository);

        Path csvPath = exportUtility.exportStoresToCsv("stores_export.csv");

        assertTrue(Files.exists(csvPath), "Expected CSV file to be created at " + csvPath);

        List<String> lines = Files.readAllLines(csvPath);
        assertTrue(lines.size() > 1, "Expected CSV to contain store data, found " + lines.size() + " lines");

        String expectedHeader = "locationId,departments,ecomStore.operatingStatus,ecomStore.isDUGStore,"
                + "ecomStore.isDeliveryStore,ecomStore.storeFeatures.isExpressStore,"
                + "ecomStore.storeFeatures.isWGDEnabled,ecomStore.storeFeatures.isFlashDugEnabled,"
                + "ecomStore.storeFeatures.isFlashDeliveryEnabled,isEcomStore,timezone,brand.name,"
                + "division.name,address.line1,address.city,address.state,address.zipcode,"
                + "phone,latitude,longitude";
        assertEquals(expectedHeader, lines.get(0), "CSV header does not match expected field list");

        System.out.println("Exported " + (lines.size() - 1) + " stores to " + csvPath.toAbsolutePath());
    }
}