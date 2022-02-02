-- SomaticView.sql
-- SQL query that mirrors SomaticView
SELECT
  sM.Mutation_ID,
  Mt.MUT_ID,
  L.hg18_Chr17_coordinates,
  L.hg19_Chr17_coordinates,
  L.hg38_Chr17_coordinates,
  L.ExonIntron,
  L.Codon_number,
  L.Genomic_nt,
  Mt.Description,
  Mt.c_description,
  Mt.g_description,
  Mt.g_description_GRCh38,
  A.ProtDescription,
  Mt.COSMIClink,
  Mt.CLINVARlink,
  Mt.TCGA_ICGC_GENIE_count,
  Mt.cBioportalCount,
  L.Splice_site,
  L.CpG_site,
  L.Context_coding_3,
  Ty.Type,
  L.Base_number AS WT_nucleotide,
  Mt.Mutant_nucleotide,
  Mt.Mut_rate,
  P.WT_codon,
  Mt.Mutant_codon,
  P.WT_AA,
  G.Amino_Acid AS Mutant_AA,
  A.Mut_rateAA,
  E.Effect,
  Mt.Polymorphism,
  A.AGVGDClass,
  A.SIFTClass,
  A.Polyphen2,
  A.REVEL,
  A.BayesDel,
  A.StructureFunctionClass,
  A.TransactivationClass,
  A.DNE_LOFclass,
  A.DNEclass,
  P.Hotspot,
  P.Structural_motif,
  S.Sample_Name,
  S.Sample_ID,
  sS.Sample_source,
  Tu.Tumor_origin,
  T.Topography,
  T.Short_topo,
  T.Topo_code,
  sT.Sub_topography,
  M.Morphology,
  M.Morpho_code,
  S.Grade,
  S.Stage,
  S.TNM,
  S.p53_IHC,
  S.KRAS_status,
  S.Other_mutations,
  S.Other_associations,
  S.Add_Info,
  I.Individual_ID,
  I.Sex,
  S.Age,
  I.Ethnicity,
  C.Country,
  C.Population,
  C.Region,
  C.Development,
  I.Geo_area,
  I.TP53polymorphism,
  I.Germline_mutation,
  I.Family_history,
  Tob.Tobacco,
  Al.Alcohol,
  Ex.Exposure,
  I.Infectious_agent,
  R.Ref_ID,
  R.PubMed,
  R.Exclude_analysis,
  R.WGS_WXS,
  A.AAchange_ID,
  Ty.Type_ID,
  E.Effect_ID,
  C.Country_ID,
  R.Cross_Ref_ID,
  R.Comment,
  R.Start_material,
  R.exon2,
  R.exon3,
  R.exon4,
  R.exon5,
  R.exon6,
  R.exon7,
  R.exon8,
  R.exon9,
  R.exon10,
  R.exon11,
  FI.WAF1nWT,
  FI.MDM2nWT,
  FI.BAXnWT,
  FI.h1433snWT,
  FI.AIP1nWT,
  FI.GADD45nWT,
  FI.NOXAnWT,
  FI.P53R2nWT,
  Mg.Morphogroup,
  Tob.Tobacco_search,
  Al.Alcohol_search,
  A.AAchange,
  sS.Sample_source_group,
  Tu.Tumor_origin_group,
  ROW_NUMBER() OVER (ORDER BY S.Sample_ID, Mt.MUT_ID) SomaticView_ID
FROM
  `isb-cgc-tp53-dev.P53_data.S_INDIVIDUAL` AS I
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Country_dic` AS C
ON
  I.Country_ID = C.Country_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.S_REFERENCE` AS R
ON
  I.Ref_ID = R.Ref_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.S_SAMPLE` AS S
ON
  I.Individual_ID = S.Individual_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Morphology_dic` AS M
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Morphogroup_dic` AS Mg
ON
  M.Morphogroup_ID = Mg.Morphogroup_ID
ON
  S.Morpho_ID = M.Morphology_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.AA_change` AS A
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.MUTATION` AS Mt
ON
  A.AAchange_ID = Mt.AAchangeID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Effect_dic` AS E
ON
  Mt.Effect_ID = E.Effect_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Genetic_code` AS G
ON
  Mt.Mutant_codon = G.Codon
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Location` AS L
ON
  Mt.Location_ID = L.Location_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.p53_sequence` AS P
ON
  L.Codon_number = P.Codon_number
  AND L.Codon_number = P.Codon_number
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.S_MUTATION` AS sM
ON
  Mt.MUT_ID = sM.MUT_ID
ON
  S.Sample_ID = sM.Sample_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Sample_source_dic` AS sS
ON
  S.Sample_source_ID = sS.Sample_source_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Subtopography_dic` AS sT
ON
  S.Subtopo_ID = sT.Subtopo_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Topography_dic` AS T
ON
  sT.Topo_code = T.Topo_code
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Tumor_origin_dic` AS Tu
ON
  S.Tumor_origin_ID = Tu.Tumor_origin_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Type_dic` AS Ty
ON
  Mt.Type_ID = Ty.Type_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Exposure_dic` AS Ex
ON
  I.Exposure_ID = Ex.Exposure_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Alcohol_dic` AS Al
ON
  I.Alcohol_ID = Al.Alcohol_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Tobacco_dic` AS Tob
ON
  I.Tobacco_ID = Tob.Tobacco_ID
LEFT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.FUNCTION_ISHIOKA` AS FI
ON
  A.AAchange_ID = FI.AAchangeID
WHERE
  (R.Exclude_analysis = FALSE)
  AND (R.Ref_ID != 2636)
  AND (R.Ref_ID != 2637)
  AND (R.Ref_ID != 2638)
ORDER BY
  T.Short_topo,
  M.Morphology,
  R.Ref_ID