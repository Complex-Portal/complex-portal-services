package uk.ac.ebi.complex.service.service;


import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


public class UniProtMappingServiceTest {

    UniProtMappingService uniProtMappingService = new UniProtMappingService();

    @Test
    public void testTrEMBLToSwissProt() {
        // Q9H8P6 has been merged into the reviewed Q9BXJ9 since 3.5 https://www.uniprot.org/uniprotkb/Q9H8P6/history
        String oldIdentifier = "Q9H8P6";
        Map<String, List<String>> mapping = uniProtMappingService.mapIds(List.of(oldIdentifier));
        List<String> ids = mapping.get(oldIdentifier);
        assertNotNull(ids, "The mapping service should have processed this identifier");
        assertEquals(1, ids.size(), "This entry should only map to one identifier");
        assertEquals("Q9BXJ9", ids.get(0), "As of 12/07/2024, Q9H8P6 should be mapped to the reviewed Q9BXJ9");
    }

    @Test
    public void testSwissProtToSwissProt() {
        // TODO find example
    }

    @Test
    public void testDeletedEntry() {
        String oldIdentifier = "F5GXM8";
        Map<String, List<String>> mapping = uniProtMappingService.mapIds(List.of(oldIdentifier));
        List<String> ids = mapping.get(oldIdentifier);
        assertNotNull(ids, "The mapping service should have processed this identifier");
        assertEquals(0, ids.size(), "This entry should not map to anything");
    }

    @Test
    public void testEntryDeMerged() {
        // P29358 has been split into P68251 and P68250. Since there is an ambiguity, we
        String oldIdentifier = "P29358";
        Map<String, List<String>> mapping = uniProtMappingService.mapIds(List.of(oldIdentifier));
        List<String> ids = mapping.get(oldIdentifier);
        assertNotNull(ids, "The mapping service should have processed this identifier");
        assertTrue( ids.size() >= 2, "This entry should map to at least 2 identifiers");
    }
}