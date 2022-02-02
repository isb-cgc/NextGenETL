-- MutationView.sql
-- SQL query to mirror MutationView

SELECT
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
  A.ProtDescription,
  M.hgvs_hg19,
  M.hgvs_hg38,
  M.hgvs_NM_000546,
  L.Splice_site,
  L.CpG_site,
  L.Context_coding_3,
  Ty.Type,
  L.Base AS WT_nucleotide,
  M.Mutant_nucleotide,
  M.Mut_rate,
  S.WT_codon,
  M.Mutant_codon,
  S.WT_AA,
  G.Amino_Acid AS Mutant_AA,
  A.Mut_rateAA,
  E.Effect,
  M.Polymorphism,
  M.COSMIClink,
  M.CLINVARlink,
  M.TCGA_ICGC_GENIE_count,
  S.Hotspot,
  P.SNPlink,
  P.gnomADlink,
  P.SourceDatabases,
  P.PubMedlink,
  S.Residue_function,
  S.Domain_function,
  S.Structural_motif,
  S.SA,
  A.TransactivationClass,
  A.DNE_LOFclass,
  A.DNEclass,
  A.StructureFunctionClass,
  A.AGVGDClass,
  A.SIFTClass,
  A.Polyphen2,
  A.BayesDel,
  A.REVEL,
  M.EffectGroup3,
  A.SwissProtLink,
  F.WAF1nWT,
  F.MDM2nWT,
  F.BAXnWT,
  F.h1433snWT,
  F.AIP1nWT,
  F.GADD45nWT,
  F.NOXAnWT,
  F.P53R2nWT,
  A.AAchange,
  F.WAF1nWT_Saos2,
  F.MDM2nWT_Saos2,
  F.BAXnWT_Saos2,
  F.h1433snWT_Saos2,
  F.AIP1nWT_Saos2,
  F.PUMAnWT_Saos2,
  F.SubG1nWT_Saos2,
  M.Type_ID,
  A.AAchange_ID
FROM
  `isb-cgc-tp53-dev.P53_data.POLYMORPHISM` AS P
RIGHT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.MUTATION` AS M
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Location` AS L
ON
  M.Location_ID = L.Location_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Genetic_code` AS G
ON
  M.Mutant_codon = G.Codon
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Type_dic` AS Ty
ON
  M.Type_ID = Ty.Type_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.AA_change` AS A
ON
  M.AAchangeID = A.AAchange_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.p53_sequence` AS S
ON
  L.Codon_number = S.Codon_number
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Effect_dic` AS E
ON
  M.Effect_ID = E.Effect_ID
ON
  P.MUT_ID = M.MUT_ID
LEFT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.FUNCTION_ISHIOKA` AS F
ON
  A.AAchange_ID = F.AAchangeID
WHERE
  (M.CompleteDescription = FALSE)
ORDER BY
  L.hg38_Chr17_coordinates DESC,
  M.Description