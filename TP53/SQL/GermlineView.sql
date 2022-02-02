-- GermlineView.sql
-- SQL query to mirror GermlineView

SELECT
  F.Family_ID,
  F.Family_code,
  Gc.Class,
  F.Generations_analyzed,
  F.Germline_mutation,
  C.Country,
  C.Population,
  C.Region,
  C.Development,
  M.MUT_ID,
  L.hg18_Chr17_coordinates,
  L.hg19_Chr17_coordinates,
  L.hg38_Chr17_coordinates,
  L.ExonIntron,
  L.Codon_number,
  M.Description,
  M.c_description,
  M.g_description,
  M.g_description_GRCh38,
  T.Type,
  L.Base AS WT_nucleotide,
  L.Splice_site,
  L.CpG_site,
  M.Mutant_nucleotide,
  S.WT_codon,
  M.Mutant_codon,
  S.WT_AA,
  Gcd.Amino_Acid AS Mutant_AA,
  E.Effect,
  A.AGVGDClass,
  A.SIFTClass,
  A.Polyphen2,
  A.REVEL,
  A.BayesDel,
  A.TransactivationClass,
  A.DNE_LOFclass,
  A.DNEclass,
  A.ProtDescription,
  M.COSMIClink,
  M.CLINVARlink,
  M.TCGA_ICGC_GENIE_count,
  S.Hotspot,
  I.Individual_ID,
  I.Individual_code,
  FC.FamilyCase,
  FC.FamilyCase_group,
  I.Sex,
  I.Germline_carrier,
  I.Mode_of_inheritance,
  I.Dead,
  I.Unaffected,
  I.Age,
  Gt.Tumor_ID,
  Tp.Topography,
  Mp.Morphology,
  Gt.Age_at_diagnosis,
  F.Ref_ID,
  R.PubMed,
  Tp.Short_topo,
  Gt.ShortTumor,
  E.Effect_ID,
  S.Structural_motif,
  T.Type_ID,
  Tp.StatisticGraphGermline,
  Mg.Morphogroup,
  A.AAchange,
  A.Mut_rateAA,
  F.Other_infos,
  ROW_NUMBER() OVER (ORDER BY Gt.Tumor_ID) GermlineView_ID
FROM
  `isb-cgc-tp53-dev.P53_data.Subtopography_dic` Su
LEFT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.Topography_dic` Tp
ON
  Su.Topo_code = Tp.Topo_code
RIGHT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.G_TUMOR` Gt
LEFT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.Morphology_dic` Mp
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Morphogroup_dic` Mg
ON
  Mp.Morphogroup_ID = Mg.Morphogroup_ID
ON
  Gt.Morpho_ID = Mp.Morphology_ID
ON
  Su.Subtopo_ID = Gt.Subtopo_ID
RIGHT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.Genetic_code` Gcd
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.G_Classification_dic` Gc
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.G_FAMILY` F
ON
  Gc.Class_ID = F.Class_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.G_INDIVIDUAL` I
ON
  F.Family_ID = I.Family_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.G_P53_MUTATION` pM
ON
  F.Family_ID = pM.Family_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.G_REFERENCE` R
ON
  F.Ref_ID = R.Ref_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Country_dic` C
ON
  F.Country_ID = C.Country_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.MUTATION` M
ON
  pM.MUT_ID = M.MUT_ID
ON
  Gcd.Codon = M.Mutant_codon
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Effect_dic` E
ON
  M.Effect_ID = E.Effect_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.AA_change` A
ON
  M.AAchangeID = A.AAchange_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Location` L
ON
  M.Location_ID = L.Location_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.p53_sequence` S
ON
  L.Codon_number = S.Codon_number
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Type_dic` T
ON
  M.Type_ID = T.Type_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.G_FamilyCase_dic` FC
ON
  I.FamilyCase_ID = FC.FamilyCase_ID
ON
  Gt.Individual_ID = I.Individual_ID
WHERE
  (LOWER(F.Germline_mutation) LIKE 'tp53%')
  AND (Tp.Short_topo IS NOT NULL)
  AND (LOWER(I.Germline_carrier) = 'confirmed'
    OR LOWER(I.Germline_carrier) = 'obligatory')
ORDER BY
  F.Ref_ID,
  F.Family_ID