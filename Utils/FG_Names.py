# Dictionary to map FG_Name to their corresponding item names, source, stage, and BOM details
fg_name_to_items = {
    '2,3 DI CHLORO PYRIDINE': {
        '2,3 DCP IPA ML': {'source': ['bi_product'], 'stage': 1},
        '2,3 DCP RECOVERED IPA': {'source': ['out_product'], 'stage': 2},
        '2,3 DCP IIND CROP': {'source': ['bi_product'], 'stage': 3},
        '2,3 DCP SFG': {'source': ['bi_product'], 'stage': 4},
        '2,3 DCP CRUDE': {'source': ['out_product'], 'stage': 5},
    },
    'N,N DI ISO PROPYL ETHYL AMINE': {
        'DIPEA INTERCUT-1': {'source': ['bi_product'], 'stage': 1},
        'DIPEA INTERCUT-2 (IIND CUT)': {'source': ['bi_product'], 'stage': 2},
        'DIPEA RESIDUE': {'source': ['bi_product','out_product'], 'stage': 3},
        'DIPEA CRUDE': {'source': ['out_product'], 'stage': 4, 'Bom': True},
        'DIPA WIP (IST CUT)': {'source': ['bi_product'], 'stage': 5},
    },
    '2,4,6 TRIMETHYL PHENYL ACETYL CHLORIDE': {
        '2,4,6 TMBCL (STAGE-I) ORGANIC LAYER': {'source': ['out_product'], 'stage': 1, 'Bom': True},
        '2,4,6 TMBCL STAGE-I': {'source': ['out_product'], 'stage': 2, 'Bom': True},
        '2,4,6 TMBCL (STAGE-I) INTERCUT-1': {'source': ['bi_product'], 'stage': 3},
        '2,4,6 TMBCN STAGE-II': {'source': ['bi_product'], 'stage': 4, 'Bom': True},
        '2,4,6 TMBCN (STAGE-II) WET CAKE': {'source': ['out_product'], 'stage': 5, 'Bom': True},
        '2,4,6 TMPACL (STAGE-III) DRY POWDER': {'source': ['out_product'], 'stage': 6, 'Bom': True},
        '2,4,6 TMPACL (STAGE-III) WET POWDER': {'source': ['out_product'], 'stage': 7, 'Bom': True},
        '2,4,6 TMPACL (STAGE-IV) CRUDE': {'source': ['out_product'], 'stage': 8, 'Bom': True},
        '2,4,6 TMPACL INTERCUT-1': {'source': ['bi_product'], 'stage': 9},
        '2,4,6 TMPACL INTERCUT-2': {'source': ['bi_product'], 'stage': 10},
        '2,4,6 TMPAC SFG': {'source': ['out_product','bi_product'], 'stage': 11},
        '2,4,6 RECOVERED MESITYLENE': {'source': ['out_product','bi_product'], 'stage': 12},
        '2,4,6 (STAGE-III) RECOVERED TOLUENE': {'source': ['out_product','bi_product'], 'stage': 13},   
        '2,4,6 (STAGE-II) RECOVERED TOLUENE': {'source': ['out_product','bi_product'], 'stage': 14},   
        '2,4,6 (STAGE-III) TOLUENE WIP': {'source': ['out_product','bi_product'], 'stage': 15},   
    },
    '2,5 DIMETHYL PHENYL ACETYL CHLORIDE': {
        '2,5 DMBCL (STAGE-I) CRUDE': {'source':  ['out_product', 'bi_product'], 'stage': 1, 'Bom': True},
        '2,5 DMBCL STAGE-I': {'source':  ['out_product', 'bi_product'], 'stage': 2, 'Bom': True},
        '2,5 DMBCL (STAGE-I) INTERCUT': {'source': ['out_product', 'bi_product'], 'stage': 3},
        '2,5 DMBCN (STAGE-II) CRUDE': {'source':  ['out_product', 'bi_product'], 'stage': 4, 'Bom': True},
        '2,5 DMBCN STAGE-II': {'source':  ['out_product', 'bi_product'], 'stage': 5, 'Bom': True},
        '2,5 DMBCN (STAGE II) INTERCUT': {'source':  ['out_product', 'bi_product'], 'stage': 6},
        '2,5 DMPAA (STAGE-III) DRY POWDER': {'source':  ['out_product', 'bi_product'], 'stage': 7, 'Bom': True},
        '2,5 DMPAC (STAGE-IV) CRUDE': {'source':  ['out_product', 'bi_product'], 'stage': 8, 'Bom': True},
        '2,5 DMBCL RECOVERED P- XYLENE': {'source':  ['out_product', 'bi_product'], 'stage': 9},
        '2,5 DMPAC SFG': {'source':  ['out_product', 'bi_product'], 'stage': 10},
        '2,5 DMPACL (STAGE-IV) INTERCUT-1': {'source':  ['out_product', 'bi_product'], 'stage': 11},
        '2,5 DMPACL (STAGE-IV) INTERCUT-2': {'source':  ['out_product', 'bi_product'], 'stage': 12},
    },
    'DICHLORO ACETIC ACID': {
        'DCAT ORGANIC': {'source': ['out_product'], 'stage': 1},
        'DCAT RECOVERED MDC': {'source': ['bi_product'], 'stage': 2},
        'DCAT INTERCUT-1': {'source': ['bi_product'], 'stage': 3},
    },
    'AMIDO CHLORIDE': {
        'AMIDO IPA ML': {'source': ['out_product', 'bi_product'], 'stage': 1},  
        'AMIDO RECOVERED ISO PROPYL ACETATE': {'source':  ['out_product', 'bi_product'], 'stage': 2},
        'AMIDO RECOVERED IPAC INTERCUT-1': {'source':  ['out_product', 'bi_product'], 'stage': 3},
        'AMIDO CHLORIDE OFF SPEC': {'source':  ['out_product', 'bi_product'], 'stage': 4},
    },
    '2-METHOXY BENZOIC ACID':{
        '2-MBA WET CAKE': {'source': ['out_product', 'bi_product'], 'stage': 1,'Bom': True},  
        '2-MBA OFF SPEC': {'source':  ['out_product', 'bi_product'], 'stage': 2},
    },
    'METCAMIFEN TECH.':{
        'METCAMIFEN SAM-I WET CAKE': {'source': ['out_product', 'bi_product'], 'stage': 1, 'Bom': True},  
        'METCAMIFEN SAM-II WET CAKE': {'source':  ['out_product', 'bi_product'], 'stage': 2},
        'METCAMIFEN SAM-II DRY POWDER': {'source':  ['out_product', 'bi_product'], 'stage': 3, 'Bom': True},
        'METCAMIFEN STAGE-III MAINCUT': {'source':  ['out_product', 'bi_product'], 'stage': 4},
        'METCAMIFEN STAGE-III OA-CI CRUDE': {'source':  ['out_product', 'bi_product'], 'stage': 5},
        'METCAMIFEN STAGE IV SAM-IV SFG OFF SPEC': {'source':  ['out_product', 'bi_product'], 'stage': 6},
        'METCAMIFEN STAGE IV SAM-IV SFG SPEC': {'source':  ['out_product', 'bi_product'], 'stage': 7},
        'METCAMIFEN STAGE-I SAM UREA DRY POWDER': {'source':  ['out_product', 'bi_product'], 'stage': 8},
        'METCAMIFEN STAGE-II OA-CI CRUDE': {'source':  ['out_product', 'bi_product'], 'stage': 9},
        'METCAMIFEN STAGE-II- OA-Cl MAINCUT': {'source':  ['out_product', 'bi_product'], 'stage': 10},
        'METCAMIFEN STAGE-II OA-CI INTERCUT-1': {'source':  ['out_product', 'bi_product'], 'stage': 11},
        'METCAMIFEN STAGE-III INTERCUT': {'source':  ['out_product', 'bi_product'], 'stage': 12},
    },
    '2,4 DICHLORO BENZOYL CHLORIDE':{
        '2,4 DICHLRO BENZAL CHLORIDE CRUDE': {'source':  ['out_product', 'bi_product'], 'stage': 1},
        '2,4 DCBC CRUDE': {'source':  ['out_product', 'bi_product'], 'stage': 2, 'Bom': True},
        '2,4 DCBC INTERCUT-1': {'source':  ['out_product', 'bi_product'], 'stage': 3},
        '2,4 DCBC INTERCUT-2': {'source':  ['out_product', 'bi_product'], 'stage': 4},
        'RECOVERED 2,4 DCT': {'source':  ['out_product', 'bi_product'], 'stage': 5},
    },
    'C-5 HYDROXY ESTER':{
        'C-5 HYDROXY ESTER CRUDE': {'source':  ['out_product', 'bi_product'], 'stage': 1, 'Bom': True},
        'C-5 INTERCUT-1': {'source':  ['out_product', 'bi_product'], 'stage': 2},
        'C-5 INTERCUT-2': {'source':  ['out_product', 'bi_product'], 'stage': 3},
        'C-5 RECOVERED TOLUENE': {'source':  ['out_product', 'bi_product'], 'stage': 4},
        'C-5 RECOVERED E.A. + TOLUENE': {'source':  ['out_product', 'bi_product'], 'stage': 5},
        'C-5 HYDROXY ESTER ORGANIC LAYER': {'source':  ['out_product', 'bi_product'], 'stage': 6},
        'C-5 HYDROXY ESTER SFG': {'source':  ['out_product', 'bi_product'], 'stage': 7},
    },
    '4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE':{
        'SPIR STAGE-I': {'source':  ['out_product', 'bi_product'], 'stage': 1,'Bom': True},
        'SPIR (STAGE-II) WET CAKE': {'source':  ['out_product', 'bi_product'], 'stage': 2,'Bom': True},
    },
    '2,4 DICHLORO BENZALDEHYDE':{
        '2,4 DICHLORO BENZALDEHYDE WET CAKE': {'source':  ['out_product', 'bi_product'], 'stage': 1},
        '2,4 DICHLRO BENZAL CHLORIDE CRUDE': {'source':  ['out_product', 'bi_product'], 'stage': 2},
    },
    '2,6 DICHLORO BENZOYL CHLORIDE':{
        '2,6 DCBC CRUDE': {'source':  ['out_product', 'bi_product'], 'stage': 1},
        '2,6 DCBC INTERCUT-1': {'source':  ['out_product', 'bi_product'], 'stage': 2},
        '2,6 DCBC INTERCUT-2': {'source':  ['out_product', 'bi_product'], 'stage': 3},
        '2,6 DCBN (STAGE-I) DRY POWDER': {'source':  ['out_product', 'bi_product'], 'stage': 4},
    },
    'DMA-CHLORIDE LAN':{
        'OCDB INTERCUT-01': {'source':  ['out_product', 'bi_product'], 'stage': 1},
        'OCDB ORGANIC LAYER': {'source':  ['out_product', 'bi_product'], 'stage': 2,'Bom': True},
    },
    '4-FLUORO-3-TRIFLUOROMETHYL PHENOL':{
        '4-FTMP CRUDE': {'source':  ['out_product', 'bi_product'], 'stage': 1},
        '4-FTMP INTERCUT-1': {'source':  ['out_product', 'bi_product'], 'stage': 2},
        '4-FTMP INTERCUT-2': {'source':  ['out_product', 'bi_product'], 'stage': 3},
        '4-FTMP RECOVERED TOLUENE': {'source':  ['out_product', 'bi_product'], 'stage': 4},
    },
    '2,6 DIMETHOXY BENZOIC ACID':{
        '2,6 DCBN (STAGE-I) DRY POWDER': {'source':  ['out_product', 'bi_product'], 'stage': 1},
        '2,6 DMBN (STAGE-II) WET POWDER': {'source':  ['out_product', 'bi_product'], 'stage': 2},
        '2,6 DMBA (STAGE-II) IIND CROP': {'source':  ['out_product', 'bi_product'], 'stage': 3},
        '2,6 DMBA (STAGE-III) IIND CROP': {'source':  ['out_product', 'bi_product'], 'stage': 4},
    },
    'METHYL-2-CHLORO PROPIONATE':{
        '2-CHLORO PROPIONIC ACID-M2CP STAGE-I': {'source':  ['out_product', 'bi_product'], 'stage': 1,'Bom': True},
        'M2CP CRUDE': {'source':  ['out_product', 'bi_product'], 'stage': 2,'Bom': True},
        'M2CP INTERCUT-1': {'source':  ['out_product', 'bi_product'], 'stage': 3},
        'M2CP INTERCUT-2': {'source':  ['out_product', 'bi_product'], 'stage': 4},
    }
}