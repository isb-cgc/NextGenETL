-- CellLineView.sql
-- SQL query to mirror CellLineView

SELECT
  sS.Sample_ID,
  sS.Sample_Name,
  Cl.ATCC_ID,
  Cl.Cosmic_ID,
  Cl.depmap_ID,
  Tp.Short_topo,
  M.Morphology,
  Tu.Tumor_origin,
  Cl.Add_info,
  I.Sex,
  sS.Age,
  C.Country,
  C.Population,
  C.Region,
  C.Development,
  I.Germline_mutation,
  I.Infectious_agent,
  Tb.Tobacco,
  Al.Alcohol,
  Ex.Exposure,
  sS.KRAS_status,
  sS.Other_mutations,
  Cl.TP53status,
  sS.LOH AS TP53_LOH,
  Cl.Protein_status AS p53_protein_status,
  sS.p53_IHC,
  Mt.MUT_ID,
  L.hg18_Chr17_coordinates,
  L.hg19_Chr17_coordinates,
  L.hg38_Chr17_coordinates,
  L.ExonIntron,
  L.Codon_number,
  Mt.Description,
  Mt.c_description,
  Mt.g_description,
  Mt.g_description_GRCh38,
  A.ProtDescription,
  Mt.COSMIClink,
  Mt.TCGA_ICGC_GENIE_count,
  Mt.cBioportalCount,
  P.Hotspot,
  Ty.Type,
  L.Base AS WT_nucleotide,
  Mt.Mutant_nucleotide,
  P.WT_codon,
  Mt.Mutant_codon,
  P.WT_AA,
  G.Amino_Acid AS Mutant_AA,
  E.Effect,
  A.AGVGDClass,
  A.SIFTClass,
  A.Polyphen2,
  A.REVEL,
  A.BayesDel,
  A.DNE_LOFclass,
  A.DNEclass,
  A.TransactivationClass,
  F.WAF1nWT,
  F.MDM2nWT,
  F.BAXnWT,
  F.AIP1nWT,
  F.h1433snWT,
  F.GADD45nWT,
  F.NOXAnWT,
  F.P53R2nWT,
  R.Ref_ID,
  R.Journal,
  R.S_Ref_Year AS Year,
  R.Volume,
  R.Start_page,
  R.PubMed,
  R.Start_material,
  R.Prescreening,
  R.Material_sequenced,
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
  Mt.Type_ID,
  P.Structural_motif,
  A.AAchange,
  Mg.Morphogroup,
  Tu.Tumor_origin_group,
  Tb.Tobacco_search,
  Al.Alcohol_search,
  Mt.hgvs_hg19,
  Mt.hgvs_hg38,
  Mt.hgvs_NM_000546
FROM
  `isb-cgc-tp53-dev.P53_data.Effect_dic` AS E
RIGHT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.AA_change` AS A
RIGHT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.MUTATION` AS Mt
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Type_dic` AS Ty
ON
  Mt.Type_ID = Ty.Type_ID
ON
  A.AAchange_ID = Mt.AAchangeID
ON
  E.Effect_ID = Mt.Effect_ID
LEFT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.Genetic_code` AS G
ON
  Mt.Mutant_codon = G.Codon
LEFT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.Location` AS L
LEFT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.p53_sequence` AS P
ON
  L.Codon_number = P.Codon_number
ON
  Mt.Location_ID = L.Location_ID
RIGHT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.S_MUTATION` AS sM
ON
  Mt.MUT_ID = sM.MUT_ID
RIGHT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.Morphogroup_dic` AS Mg
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.CellLines` AS Cl
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.S_REFERENCE` AS R
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.S_INDIVIDUAL` AS I
ON
  R.Ref_ID = I.Ref_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.S_SAMPLE` AS sS
ON
  I.Individual_ID = sS.Individual_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Morphology_dic` AS M
ON
  sS.Morpho_ID = M.Morphology_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Subtopography_dic` AS sT
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Topography_dic` AS Tp
ON
  sT.Topo_code = Tp.Topo_code
ON
  sS.Subtopo_ID = sT.Subtopo_ID
ON
  Cl.Sample_ID = sS.Sample_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Country_dic` AS C
ON
  I.Country_ID = C.Country_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Exposure_dic` AS Ex
ON
  I.Exposure_ID = Ex.Exposure_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Tobacco_dic` AS Tb
ON
  I.Tobacco_ID = Tb.Tobacco_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Alcohol_dic` AS Al
ON
  I.Alcohol_ID = Al.Alcohol_ID
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.Tumor_origin_dic` AS Tu
ON
  sS.Tumor_origin_ID = Tu.Tumor_origin_ID
ON
  Mg.Morphogroup_ID = M.Morphogroup_ID
ON
  sM.Sample_ID = sS.Sample_ID
LEFT OUTER JOIN
  `isb-cgc-tp53-dev.P53_data.FUNCTION_ISHIOKA` AS F
ON
  A.AAchange_ID = F.AAchangeID
WHERE
  (sS.Sample_source_ID = 4)
ORDER BY
  Tp.Short_topo,
  sS.Sample_Name,
  Cl.TP53status