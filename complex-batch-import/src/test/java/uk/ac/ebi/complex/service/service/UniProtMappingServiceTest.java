package uk.ac.ebi.complex.service.service;


import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import uk.ac.ebi.complex.service.model.UniprotProtein;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


public class UniProtMappingServiceTest {

    UniProtMappingService uniProtMappingService = new UniProtMappingService();

    @Test
    public void testTrEMBLToSwissProt() {
        // Q9H8P6 has been merged into the reviewed Q9BXJ9 since 3.5 https://www.uniprot.org/uniprotkb/Q9H8P6/history
        String oldIdentifier = "Q9H8P6";
        Map<String, List<UniprotProtein>> mapping = uniProtMappingService.mapIds(List.of(oldIdentifier));
        List<UniprotProtein> ids = mapping.get(oldIdentifier);
        assertNotNull(ids, "The mapping service should have processed this identifier");
        assertEquals(1, ids.size(), "This entry should only map to one identifier");
        assertEquals("Q9BXJ9", ids.get(0).getProteinAc(), "As of 12/07/2024, Q9H8P6 should be mapped to the reviewed Q9BXJ9");
    }

    @Test
    public void testSwissProtToSwissProt() {
        // TODO find example
    }

    @Test
    public void testDeletedEntry() {
        String oldIdentifier = "F5GXM8";
        Map<String, List<UniprotProtein>> mapping = uniProtMappingService.mapIds(List.of(oldIdentifier));
        List<UniprotProtein> ids = mapping.get(oldIdentifier);
        assertNotNull(ids, "The mapping service should have processed this identifier");
        assertEquals(0, ids.size(), "This entry should not map to anything");
    }

    @Test
    public void testEntryDeMerged() {
        // P29358 has been split into P68251 and P68250. Since there is an ambiguity, we
        String oldIdentifier = "P29358";
        Map<String, List<UniprotProtein>> mapping = uniProtMappingService.mapIds(List.of(oldIdentifier));
        List<UniprotProtein> ids = mapping.get(oldIdentifier);
        assertNotNull(ids, "The mapping service should have processed this identifier");
        assertTrue( ids.size() >= 2, "This entry should map to at least 2 identifiers");
    }

    @Test
    public void testMapGenes() {
        Map<String, String> genesToMap = ImmutableMap.<String, String>builder()
                .put("ZNF576", "Q9H609")
                .put("SNX30", "Q5VWJ9")
                .put("LIG1", "P18858")
                .put("SMAP1", "Q8IYB5")
                .put("SF1", "Q15637")
                .put("TRAF2", "Q12933")
                .put("SOCS6", "O14544")
                .put("SOCS4", "Q8WXH5")
                .put("MAGEE1", "Q9HCI5")
                .put("ARHGAP10", "A1A4S6")
                .put("BRIP1", "Q9BX63")
                .put("EDH2", "Q9NZN4")
                .put("WDR11", "Q9BZH6")
                .put("CST6", "Q15828")
                .put("MAGI1", "Q96QZ7")
                .put("POP1", "Q99575")
                .put("POP4", "O95707")
                .put("MPP7", "Q5T2T1")
                .put("CLP1", "Q92989")
                .put("LIG3", "P49916")
                .put("LAT2", "Q9GZY6")
                .put("LRRN2", "O75325")
                .put("SMAP2", "Q8WU79")
                .put("FLOT1", "O75955")
                .put("TTF2", "Q9UNY4")
                .put("RANBP9", "Q96S59")
                .put("HBP1", "O60381")
                .put("RFC1", "P35251")
                .put("NAF1", "Q96HR8")
                .put("CCNL1", "Q9UK58")
                .put("MED1", "Q15648")
                .put("MED25", "Q71SY5")
                .put("POL1RD", "P0DPB6")
                .put("TEP1", "Q99973")
                .put("MGA", "Q8IWI9")
                .put("SPIN1", "Q9Y657")
                .put("TCF4", "P15884")
                .put("TCF3", "P15923")
                .put("PAF1", "Q8N7H5")
                .put("NRP1", "O14786")
                .put("MRPL28", "Q13084")
                .put("MRPL15", "Q9P015")
                .put("DAP3", "P51398")
                .put("USP10", "Q14694")
                .put("PPOX", "P50336")
                .put("CHP1", "Q99653")
                .put("SPC25", "Q9HBM1")
                .put("TNNC1", "P63316")
                .put("SAP130", "Q9H0E3")
                .put("ERCC6", "Q03468")
                .put("RNH1", "P13489")
                .put("CDKN3", "Q16667")
                .put("RGP1", "Q92546")
                .put("CDS2", "O95674")
                .put("CHD5", "Q8TDI0")
                .put("AK6", "Q9Y3D8")
                .put("NSL1", "Q96IY1")
                .put("PRPF4B", "Q13523")
                .put("RBM10", "P98175")
                .put("PAK5", "Q9P286")
                .put("POLR1E", "Q9GZS1")
                .build();

        Map<String, List<UniprotProtein>> mapping = uniProtMappingService.mapGenes(genesToMap.keySet());
        mapping.forEach((gene, proteins) -> {
            assertEquals(1, proteins.size());
            assertEquals(genesToMap.get(gene), proteins.get(0).getProteinAc());
        });
    }

    @Test
    public void testMapGenesWithAmbiguousMappings() {
        Map<String, String> genesToMap = ImmutableMap.<String, String>builder()
                .put("AKAP7", "O43687")
                .put("TOR1AIP2", "Q8NFQ8")
                .put("PMF1-BGLAP", "U3KQ54")
                .put("POLR2M", "P0CAP2")
                .put("POLR1D", "P0DPB6")
                .put("ZNF689", "Q96CS4")
                .put("COMMD3-BMI1", "R4GMX3")
                .put("GNAS", "Q5JWF2")
                .build();

        Map<String, List<UniprotProtein>> mapping = uniProtMappingService.mapGenes(genesToMap.keySet());
        mapping.forEach((gene, proteins) -> {
            assertEquals(1, proteins.size());
            assertEquals(genesToMap.get(gene), proteins.get(0).getProteinAc());
        });
    }

    @Test
    public void testMapIdentifiers() {
        Map<String, String> idsToMap = ImmutableMap.<String, String>builder()
                .put("O75663", "O75663")
                .put("O75663-2", "O75663")
                .put("O75150", "O75150")
                .put("O75150-3", "O75150")
                .put("O75150-4", "O75150")
                .build();

        Map<String, List<UniprotProtein>> mapping = uniProtMappingService.mapIds(idsToMap.keySet());
        mapping.forEach((id, proteins) -> {
            assertEquals(1, proteins.size());
            assertEquals(idsToMap.get(id), proteins.get(0).getProteinAc());
        });
    }
}