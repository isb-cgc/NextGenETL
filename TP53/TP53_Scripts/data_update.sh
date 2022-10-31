#copy BQ tables or gs bucket files from tier to tier
SOURCE_TIER=-test
DEST_TIER=

#SOURCE_GCP_NAME=isb-cgc-tp53-dev
SOURCE_GCP_NAME=isb-cgc-tp53-test
#DEST_GCP_NAME=isb-cgc-tp53-test
DEST_GCP_NAME=isb-cgc-tp53

#back up copies
gsutil mv gs://tp53-static-files${DEST_TIER}/data/GermlineDownload_r20.csv gs://tp53-static-files${DEST_TIER}/data/GermlineDownload_r20.csv.bk
gsutil mv gs://tp53-static-files${DEST_TIER}/data/MutationView_r20.csv gs://tp53-static-files${DEST_TIER}/data/MutationView_r20.csv.bk
gsutil mv gs://tp53-static-files${DEST_TIER}/data/SomaticDownload_r20.csv gs://tp53-static-files${DEST_TIER}/data/TumorVariantDownload_r20.csv.bk

#rename SomaticRefDownload to TumorVariantRefDownload
gsutil mv gs://tp53-static-files${DEST_TIER}/data/SomaticRefDownload_r20.csv gs://tp53-static-files${DEST_TIER}/data/TumorVariantRefDownload_r20.csv


#copy gs bucket files ${SCRIPT_NAME}
gsutil cp gs://tp53-static-files${SOURCE_TIER}/data/GermlineDownload_r20.csv gs://tp53-static-files${DEST_TIER}/data/GermlineDownload_r20.csv
gsutil cp gs://tp53-static-files${SOURCE_TIER}/data/MutationView_r20.csv gs://tp53-static-files${DEST_TIER}/data/MutationView_r20.csv
gsutil cp gs://tp53-static-files${SOURCE_TIER}/data/TumorVariantDownload_r20.csv gs://tp53-static-files${DEST_TIER}/data/TumorVariantDownload_r20.csv


#copy tables
bq cp -f $SOURCE_GCP_NAME:P53_data.SpliceAI_Prediction $DEST_GCP_NAME:P53_data.SpliceAI_Prediction
bq cp -f $SOURCE_GCP_NAME:P53_data.FUNCTION_ISHIOKA $DEST_GCP_NAME:P53_data.FUNCTION_ISHIOKA
bq cp -f $SOURCE_GCP_NAME:P53_data.G_REFERENCE $DEST_GCP_NAME:P53_data.G_REFERENCE
bq cp -f $SOURCE_GCP_NAME:P53_data.GermlineRefView $DEST_GCP_NAME:P53_data.GermlineRefView
bq cp -f $SOURCE_GCP_NAME:P53_data.GermlineView $DEST_GCP_NAME:P53_data.GermlineView
bq cp -f $SOURCE_GCP_NAME:P53_data.MUTATION $DEST_GCP_NAME:P53_data.MUTATION
bq cp -f $SOURCE_GCP_NAME:P53_data.SomaticView $DEST_GCP_NAME:P53_data.SomaticView
bq cp -f $SOURCE_GCP_NAME:P53_data.MutationView $DEST_GCP_NAME:P53_data.MutationView



#rm old views

bq rm -f $DEST_GCP_NAME:P53_data.GermlineDownload
bq rm -f $DEST_GCP_NAME:P53_data.SomaticDownload

#re-create views
bq mk \
--use_legacy_sql=false \
--view \
'SELECT
  fam.Family_ID,
  fam.Family_code,
  c.Country,
  c.Population,
  c.Region,
  c.Development,
  cl.Class,
  fam.Generations_analyzed,
  fam.Germline_mutation,
  mut.MUT_ID,
  loc.hg18_Chr17_coordinates,
  loc.hg19_Chr17_coordinates,
  loc.hg38_Chr17_coordinates,
  loc.ExonIntron,
  loc.Codon_number,
  ty.Type,
  m.Description,
  m.c_description,
  m.g_description,
  m.g_description_GRCh38,
  loc.Base AS WT_nucleotide,
  m.Mutant_nucleotide,
  seq.WT_codon,
  m.Mutant_codon,
  loc.CpG_site,
  loc.Splice_site,
  loc.Context_coding_3,
  seq.WT_AA,
  code.Amino_Acid AS Mutant_AA,
  eff.Effect,
  aac.AGVGDClass,
  aac.SIFTClass,
  aac.Polyphen2,
  aac.REVEL,
  aac.BayesDel,
  aac.TransactivationClass,
  aac.DNE_LOFclass,
  aac.DNEclass,
  aac.ProtDescription,
  m.COSMIClink,
  m.CLINVARlink,
  m.TCGA_ICGC_GENIE_count,
  seq.Hotspot,
  seq.Domain_function,
  seq.Residue_function,
  gi.Individual_ID,
  gi.Individual_code,
  fc.FamilyCase,
  fc.FamilyCase_group,
  gi.Generation,
  gi.Sex,
  gi.Germline_carrier,
  gi.Mode_of_inheritance,
  gi.Dead,
  gi.Unaffected,
  gi.Age,
  gt.Tumor_ID,
  td.Topography,
  td.Short_topo,
  md.Morphology,
  gt.Age_at_diagnosis,
  fam.Ref_ID,
  fam.Other_infos,
  mut.p53mut_ID,
  gt.Add_Info,
  spai.DS_AG AS SpliceAI_DS_AG,
  spai.DS_AL AS SpliceAI_DS_AL,
  spai.DS_DG AS SpliceAI_DS_DG,
  spai.DS_DL AS SpliceAI_DS_DL,
  spai.DP_AG AS SpliceAI_DP_AG,
  spai.DP_AL AS SpliceAI_DP_AL,
  spai.DP_DG AS SpliceAI_DP_DG,
  spai.DP_DL AS SpliceAI_DP_DL,
  CASE
    WHEN ref.PubMed IS NULL OR LOWER(ref.PubMed) = "na" THEN ""
    ELSE CONCAT("https://www.ncbi.nlm.nih.gov/pubmed/",ref.PubMed)
  END AS PubMedLink
FROM
  `'$DEST_GCP_NAME'.P53_data.G_FamilyCase_dic` AS fc
RIGHT OUTER JOIN
  `'$DEST_GCP_NAME'.P53_data.G_INDIVIDUAL` AS gi
ON
  fc.FamilyCase_ID = gi.FamilyCase_ID
RIGHT OUTER JOIN
  `'$DEST_GCP_NAME'.P53_data.Effect_dic` AS eff
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.G_Classification_dic` AS cl
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.G_FAMILY` AS fam
ON
  cl.Class_ID = fam.Class_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.G_P53_MUTATION` AS mut
ON
  fam.Family_ID = mut.Family_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.G_REFERENCE` AS ref
ON
  fam.Ref_ID = ref.Ref_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Country_dic` AS c
ON
  fam.Country_ID = c.Country_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.MUTATION` AS m
ON
  mut.MUT_ID = m.MUT_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Genetic_code` AS code
ON
  m.Mutant_codon = code.Codon
ON
  eff.Effect_ID = m.Effect_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.AA_change` AS aac
ON
  m.AAchangeID = aac.AAchange_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Location` AS loc
ON
  m.Location_ID = loc.Location_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.p53_sequence` AS seq
ON
  loc.Codon_number = seq.Codon_number
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.AA_codes` AS aa
ON
  seq.WT_AA = aa.Three_letter_code
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Type_dic` AS ty
ON
  m.Type_ID = ty.Type_ID
ON
  gi.Family_ID = fam.Family_ID
LEFT OUTER JOIN
  `'$DEST_GCP_NAME'.P53_data.Subtopography_dic` AS sub
LEFT OUTER JOIN
  `'$DEST_GCP_NAME'.P53_data.Topography_dic` AS td
ON
  sub.Topo_code = td.Topo_code
RIGHT OUTER JOIN
  `'$DEST_GCP_NAME'.P53_data.Morphology_dic` AS md
RIGHT OUTER JOIN
  `'$DEST_GCP_NAME'.P53_data.G_TUMOR` AS gt
ON
  md.Morphology_ID = gt.Morpho_ID
ON
  sub.Subtopo_ID = gt.Subtopo_ID
ON
  gi.Individual_ID = gt.Individual_ID
LEFT JOIN
  `'$DEST_GCP_NAME'.P53_data.SpliceAI_Prediction` AS spai
ON
  spai.cDNA = m.c_description
WHERE
  (fam.Germline_mutation LIKE "TP53%")
ORDER BY
  fam.Family_ID,
  fam.Ref_ID' \
$DEST_GCP_NAME:P53_data.GermlineDownload


bq mk \
--use_legacy_sql=false \
--view \
'SELECT
  sm.Mutation_ID,
  m.MUT_ID,
  loc.hg18_Chr17_coordinates,
  loc.hg19_Chr17_coordinates,
  loc.hg38_Chr17_coordinates,
  loc.ExonIntron,
  loc.Codon_number,
  m.Description,
  m.c_description,
  m.g_description,
  m.g_description_GRCh38,
  loc.Base AS WT_nucleotide,
  m.Mutant_nucleotide,
  loc.Splice_site,
  loc.CpG_site,
  loc.Context_coding_3,
  ty.Type,
  m.Mut_rate,
  seq.WT_codon,
  m.Mutant_codon,
  seq.WT_AA,
  code.Amino_Acid AS Mutant_AA,
  aac.ProtDescription,
  m.COSMIClink,
  m.CLINVARlink,
  m.TCGA_ICGC_GENIE_count,
  aac.Mut_rateAA,
  eff.Effect,
  aac.AGVGDClass,
  aac.SIFTClass,
  aac.Polyphen2,
  aac.REVEL,
  aac.BayesDel,
  aac.TransactivationClass,
  aac.DNE_LOFclass,
  aac.DNEclass,
  aac.StructureFunctionClass,
  seq.Hotspot,
  seq.Structural_motif,
  s.Sample_Name,
  s.Sample_ID,
  ss.Sample_source,
  tod.Tumor_origin,
  td.Topography,
  td.Short_topo,
  td.Topo_code,
  sub.Sub_topography,
  md.Morphology,
  md.Morpho_code,
  s.Grade,
  s.Stage,
  s.TNM,
  s.p53_IHC,
  s.KRAS_status,
  s.Other_mutations,
  s.Other_associations,
  s.Add_Info,
  si.Individual_ID,
  si.Sex,
  s.Age,
  si.Ethnicity,
  si.Geo_area,
  c.Country,
  c.Development,
  c.Population,
  c.Region,
  si.TP53polymorphism,
  si.Germline_mutation,
  si.Family_history,
  tob.Tobacco,
  al.Alcohol,
  exp.Exposure,
  si.Infectious_agent,
  spai.DS_AG AS SpliceAI_DS_AG,
  spai.DS_AL AS SpliceAI_DS_AL,
  spai.DS_DG AS SpliceAI_DS_DG,
  spai.DS_DL AS SpliceAI_DS_DL,
  spai.DP_AG AS SpliceAI_DP_AG,
  spai.DP_AL AS SpliceAI_DP_AL,
  spai.DP_DG AS SpliceAI_DP_DG,
  spai.DP_DL AS SpliceAI_DP_DL,
  ref.Ref_ID,
  ref.Cross_Ref_ID,
  ref.PubMed,
  ref.Exclude_analysis,
  ref.WGS_WXS,
  CASE
    WHEN ref.PubMed IS NULL OR LOWER(ref.PubMed) = "na" THEN ""
  ELSE
  CONCAT("https://www.ncbi.nlm.nih.gov/pubmed/",ref.PubMed)
END
  AS PubMedLink
FROM
  `'$DEST_GCP_NAME'.P53_data.S_INDIVIDUAL` AS si
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Country_dic` AS c
ON
  si.Country_ID = c.Country_ID
  AND si.Country_ID = c.Country_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.S_REFERENCE` ref
ON
  si.Ref_ID = ref.Ref_ID
  AND si.Ref_ID = ref.Ref_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.S_SAMPLE` AS s
ON
  si.Individual_ID = s.Individual_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Morphology_dic` AS md
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Morphogroup_dic` AS mgd
ON
  md.Morphogroup_ID = mgd.Morphogroup_ID
ON
  s.Morpho_ID = md.Morphology_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.AA_change` AS aac
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.MUTATION` AS m
ON
  aac.AAchange_ID = m.AAchangeID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Effect_dic` AS eff
ON
  m.Effect_ID = eff.Effect_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Genetic_code` AS code
ON
  m.Mutant_codon = code.Codon
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Location` AS loc
ON
  m.Location_ID = loc.Location_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.p53_sequence` AS seq
ON
  loc.Codon_number = seq.Codon_number
  AND loc.Codon_number = seq.Codon_number
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.S_MUTATION` AS sm
ON
  m.MUT_ID = sm.MUT_ID
ON
  s.Sample_ID = sm.Sample_ID
  AND s.Sample_ID = sm.Sample_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Sample_source_dic` AS ss
ON
  s.Sample_source_ID = ss.Sample_source_ID
  AND s.Sample_source_ID = ss.Sample_source_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Subtopography_dic` AS sub
ON
  s.Subtopo_ID = sub.Subtopo_ID
  AND s.Subtopo_ID = sub.Subtopo_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Topography_dic` AS td
ON
  sub.Topo_code = td.Topo_code
  AND sub.Topo_code = td.Topo_code
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Tumor_origin_dic` AS tod
ON
  s.Tumor_origin_ID = tod.Tumor_origin_ID
  AND s.Tumor_origin_ID = tod.Tumor_origin_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Type_dic` AS ty
ON
  m.Type_ID = ty.Type_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Exposure_dic` AS exp
ON
  si.Exposure_ID = exp.Exposure_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Alcohol_dic` AS al
ON
  si.Alcohol_ID = al.Alcohol_ID
INNER JOIN
  `'$DEST_GCP_NAME'.P53_data.Tobacco_dic` AS tob
ON
  si.Tobacco_ID = tob.Tobacco_ID
LEFT JOIN
  `'$DEST_GCP_NAME'.P53_data.SpliceAI_Prediction` AS spai
ON
  spai.cDNA = m.c_description
WHERE
  (ref.Ref_ID != 2636)
  AND (ref.Ref_ID != 2637)
  AND (ref.Ref_ID != 2638)
ORDER BY
  sm.Mutation_ID' \
$DEST_GCP_NAME:P53_data.SomaticDownload


bq mk \
--use_legacy_sql=false \
--view \
'SELECT
  *
FROM
  `'$DEST_GCP_NAME'.P53_data.GermlineView`
WHERE
  (LOWER(Germline_carrier) = "confirmed"
    OR LOWER(Germline_carrier) = "obligatory")
  AND Short_topo IS NOT NULL' \
$DEST_GCP_NAME:P53_data.GermlineView_Carriers
