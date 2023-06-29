
from google.cloud import bigquery
# from google.cloud.bigquery import query
# from google.cloud.bigquery import client
import os

def generate_table(view_id, query):
    project = os.environ.get('GCLOUD_PROJECT', 'isb-cgc-tp53-test')
    client = bigquery.Client(project=project)

    view = bigquery.Table(view_id)
    view.view_query = query 
    
    view = client.create_table(view, exists_ok=True)
    
    print(f"Created {view.table_type}: {str(view.reference)}")


def somaticTumorStats(partial_id):
    view_id = f'{partial_id}.SomaticTumorStats'
    view_query = f'''
    SELECT    
    topo.StatisticGraph, topo.Short_topo, COUNT(topo.Short_topo) AS DatasetRx, ref.Exclude_analysis
    FROM         
    `{partial_id}.S_MUTATION` as s INNER JOIN
    `{partial_id}.S_SAMPLE` as sam ON s.Sample_ID = sam.Sample_ID INNER JOIN
    `{partial_id}.Subtopography_dic` as sub ON sam.Subtopo_ID = sub.Subtopo_ID INNER JOIN
    `{partial_id}.Topography_dic` as topo ON sub.Topo_code = topo.Topo_code INNER JOIN
    `{partial_id}.S_INDIVIDUAL` as ind ON sam.Individual_ID = ind.Individual_ID INNER JOIN
    `{partial_id}.S_REFERENCE` ref ON ind.Ref_ID = ref.Ref_ID
    WHERE     
    (ref.Ref_ID != 2636) AND (ref.Ref_ID != 2637) AND (ref.Ref_ID != 2638)
    GROUP BY 
    topo.StatisticGraph, ref.Exclude_analysis, topo.Short_topo
    HAVING      
    (ref.Exclude_analysis = FALSE)
    ORDER BY 
    COUNT(topo.Short_topo) DESC
    
    '''
    
    return view_id, view_query

def somaticRefDownload(partial_id):
    view_id = f'{partial_id}.SomaticRefDownload'    
    view_query = f'''
    SELECT
    ref.Ref_ID,
    ref.Cross_Ref_ID,
    ref.Title,
    ref.Authors,
    ref.S_Ref_Year AS Year,
    ref.Journal,
    ref.Volume,
    ref.Start_page,
    ref.End_page,
    ref.PubMed,
    ref.Comment,
    ref.Tissue_processing,
    ref.Start_material,
    ref.Prescreening,
    ref.Material_sequenced,
    ref.exon2,
    ref.exon3,
    ref.exon4,
    ref.exon5,
    ref.exon6,
    ref.exon7,
    ref.exon8,
    ref.exon9,
    ref.exon10,
    ref.exon11,
    ref.Exclude_analysis,
    ref.WGS_WXS
    FROM
    `{partial_id}.S_REFERENCE` AS ref
    INNER JOIN
    `{partial_id}.S_INDIVIDUAL` AS si
    ON
    ref.Ref_ID = si.Ref_ID
    WHERE
    (ref.Ref_ID != 2636
        AND ref.Ref_ID != 2637
        AND ref.Ref_ID != 2638)
    GROUP BY
    ref.Ref_ID,
    ref.Cross_Ref_ID,
    ref.Title,
    ref.Authors,
    Year,
    ref.Journal,
    ref.Volume,
    ref.Start_page,
    ref.End_page,
    ref.PubMed,
    ref.Comment,
    ref.Tissue_processing,
    ref.Start_material,
    ref.Prescreening,
    ref.Material_sequenced,
    ref.exon2,
    ref.exon3,
    ref.exon4,
    ref.exon5,
    ref.exon6,
    ref.exon7,
    ref.exon8,
    ref.exon9,
    ref.exon10,
    ref.exon11,
    ref.Exclude_analysis,
    ref.WGS_WXS
    ORDER BY
    ref.Ref_ID
    '''
    
    return view_id, view_query 
    
def somaticDownload(partial_id):
    view_id = f'{partial_id}.SomaticDownload'    
    view_query = f'''
    SELECT
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
    ref.Ref_ID,
    ref.Cross_Ref_ID,
    ref.PubMed,
    ref.Exclude_analysis,
    ref.WGS_WXS,
    CONCAT('https://www.ncbi.nlm.nih.gov/pubmed/',ref.PubMed) AS PubMedLink
    FROM
    `{partial_id}.S_INDIVIDUAL` AS si
    INNER JOIN
    `{partial_id}.Country_dic` AS c
    ON
    si.Country_ID = c.Country_ID
    AND si.Country_ID = c.Country_ID
    INNER JOIN
    `{partial_id}.S_REFERENCE` ref
    ON
    si.Ref_ID = ref.Ref_ID
    AND si.Ref_ID = ref.Ref_ID
    INNER JOIN
    `{partial_id}.S_SAMPLE` AS s
    ON
    si.Individual_ID = s.Individual_ID
    INNER JOIN
    `{partial_id}.Morphology_dic` AS md
    INNER JOIN
    `{partial_id}.Morphogroup_dic` AS mgd
    ON
    md.Morphogroup_ID = mgd.Morphogroup_ID
    ON
    s.Morpho_ID = md.Morphology_ID
    INNER JOIN
    `{partial_id}.AA_change` AS aac
    INNER JOIN
    `{partial_id}.MUTATION` AS m
    ON
    aac.AAchange_ID = m.AAchangeID
    INNER JOIN
    `{partial_id}.Effect_dic` AS eff
    ON
    m.Effect_ID = eff.Effect_ID
    INNER JOIN
    `{partial_id}.Genetic_code` AS code
    ON
    m.Mutant_codon = code.Codon
    INNER JOIN
    `{partial_id}.Location` AS loc
    ON
    m.Location_ID = loc.Location_ID
    INNER JOIN
    `{partial_id}.p53_sequence` AS seq
    ON
    loc.Codon_number = seq.Codon_number
    AND loc.Codon_number = seq.Codon_number
    INNER JOIN
    `{partial_id}.S_MUTATION` AS sm
    ON
    m.MUT_ID = sm.MUT_ID
    ON
    s.Sample_ID = sm.Sample_ID
    AND s.Sample_ID = sm.Sample_ID
    INNER JOIN
    `{partial_id}.Sample_source_dic` AS ss
    ON
    s.Sample_source_ID = ss.Sample_source_ID
    AND s.Sample_source_ID = ss.Sample_source_ID
    INNER JOIN
    `{partial_id}.Subtopography_dic` AS sub
    ON
    s.Subtopo_ID = sub.Subtopo_ID
    AND s.Subtopo_ID = sub.Subtopo_ID
    INNER JOIN
    `{partial_id}.Topography_dic` AS td
    ON
    sub.Topo_code = td.Topo_code
    AND sub.Topo_code = td.Topo_code
    INNER JOIN
    `{partial_id}.Tumor_origin_dic` AS tod
    ON
    s.Tumor_origin_ID = tod.Tumor_origin_ID
    AND s.Tumor_origin_ID = tod.Tumor_origin_ID
    INNER JOIN
    `{partial_id}.Type_dic` AS ty
    ON
    m.Type_ID = ty.Type_ID
    INNER JOIN
    `{partial_id}.Exposure_dic` AS exp
    ON
    si.Exposure_ID = exp.Exposure_ID
    INNER JOIN
    `{partial_id}.Alcohol_dic` AS al
    ON
    si.Alcohol_ID = al.Alcohol_ID
    INNER JOIN
    `{partial_id}.Tobacco_dic` AS tob
    ON
    si.Tobacco_ID = tob.Tobacco_ID
    WHERE
    (ref.Ref_ID != 2636)
    AND (ref.Ref_ID != 2637)
    AND (ref.Ref_ID != 2638)
    ORDER BY
    ref.Ref_ID
    
    '''
    
    return view_id, view_query 

def prognosisDownload(partial_id):
    view_id = f'{partial_id}.PrognosisDownload'    
    view_query = f'''
    SELECT
    pr.Prognosis_ID,
    td.Topography,
    td.Short_topo,
    md.Morphology,
    c.Country,
    c.Population,
    c.Region,
    c.Development,
    pr.Institution,
    pr.Period,
    pr.InclusionCriteria,
    pr.Treatment,
    pr.MedianFU,
    pr.RangeFU,
    pr.Cohort,
    pr.P53_mutation,
    pr.Parameter_analysed,
    pr.Association,
    pr.Result,
    ref.Ref_ID,
    ref.Cross_Ref_ID,
    ref.Title,
    ref.Authors,
    ref.S_Ref_Year,
    ref.Journal,
    ref.Volume,
    ref.Start_page,
    ref.End_page,
    ref.PubMed,
    ref.Comment,
    ref.Tissue_processing,
    ref.Start_material,
    ref.Prescreening,
    ref.Material_sequenced,
    ref.exon2,
    ref.exon3,
    ref.exon4,
    ref.exon5,
    ref.exon6,
    ref.exon7,
    ref.exon8,
    ref.exon9,
    ref.exon10,
    ref.exon11,
    ref.Exclude_analysis
    FROM
    `{partial_id}.S_PROGNOSIS` AS pr
    INNER JOIN
    `{partial_id}.S_REFERENCE` AS ref
    ON
    pr.Ref_ID = ref.Ref_ID
    INNER JOIN
    `{partial_id}.Topography_dic` AS td
    ON
    pr.Topo_code = td.Topo_code
    INNER JOIN
    `{partial_id}.Morphology_dic` AS md
    ON
    pr.Morpho_ID = md.Morphology_ID
    INNER JOIN
    `{partial_id}.Country_dic` AS c
    ON
    pr.Country_ID = c.Country_ID
    ORDER BY
    ref.Ref_ID
    
    '''
    
    return view_id, view_query 
    
def prevalenceView(partial_id):
    view_id = f'{partial_id}.PrevalenceView'    
    view_query = f'''
    SELECT
    s_prev.Prevalence_ID,
    topo_dic.Topography,
    s_prev.Topo_code,
    morph_dic.Morphology,
    morph_dic.Morpho_code,
    s_prev.Sample_analyzed,
    s_prev.Sample_mutated,
    country_dic.Country,
    country_dic.Population,
    country_dic.Region,
    country_dic.Development,
    s_prev.Comment,
    s_ref.Ref_ID,
    s_ref.Cross_Ref_ID,
    s_ref.Title,
    s_ref.Authors,
    s_ref.S_Ref_Year,
    s_ref.Journal,
    s_ref.Volume,
    s_ref.Start_page,
    s_ref.End_page,
    s_ref.PubMed,
    s_ref.Comment AS Ref_comment,
    s_ref.Tissue_processing,
    s_ref.Start_material,
    s_ref.Prescreening,
    s_ref.Material_sequenced,
    s_ref.exon2,
    s_ref.exon3,
    s_ref.exon4,
    s_ref.exon5,
    s_ref.exon6,
    s_ref.exon7,
    s_ref.exon8,
    s_ref.exon9,
    s_ref.exon10,
    s_ref.exon11,
    topo_dic.Short_topo,
    morph_grp.Morphogroup,
    country_dic.Country_ID,
    s_ref.Exclude_analysis,
    CAST(s_prev.Sample_mutated AS FLOAT64) / CAST(s_prev.Sample_analyzed AS FLOAT64) *100 AS Prevalence,
    s_ref.WGS_WXS
    FROM
    `{partial_id}.S_PREVALENCE` s_prev
    INNER JOIN
    `{partial_id}.S_REFERENCE` s_ref
    ON
    s_prev.Ref_ID = s_ref.Ref_ID
    INNER JOIN
    `{partial_id}.Topography_dic` topo_dic
    ON
    s_prev.Topo_code = topo_dic.Topo_code
    INNER JOIN
    `{partial_id}.Morphology_dic` morph_dic
    ON
    s_prev.Morpho_ID = morph_dic.Morphology_ID
    INNER JOIN
    `{partial_id}.Country_dic` country_dic
    ON
    s_prev.Country_ID = country_dic.Country_ID
    INNER JOIN
    `{partial_id}.Morphogroup_dic` morph_grp
    ON
    morph_dic.Morphogroup_ID = morph_grp.Morphogroup_ID
    WHERE
    (s_ref.Exclude_analysis = FALSE)
    '''
    
    return view_id, view_query 
    
def prevalenceStat(partial_id):
    view_id = f'{partial_id}.PrevalenceStat'    
    view_query = f'''
    SELECT
    topo.GroupOfOrgan AS Topography,
    SUM(prevDL.Sample_analyzed) AS Sample_analyzed,
    SUM(prevDL.Sample_mutated) AS Sample_mutated
    FROM
    `{partial_id}.PrevalenceDownload` AS prevDL
    INNER JOIN
    `{partial_id}.Topography_dic` AS topo
    ON
    prevDL.Topo_code = topo.Topo_code
    WHERE
    (topo.GroupOfOrgan IS NOT NULL)
    AND (prevDL.Exclude_analysis = FALSE)
    GROUP BY
    topo.GroupOfOrgan
    ORDER BY
    SUM(prevDL.Sample_mutated) DESC
    
    '''
    
    return view_id, view_query 
    
def prevalenceDownload(partial_id):
    view_id = f'{partial_id}.PrevalenceDownload'    
    view_query = f'''
    SELECT
    s_prev.Prevalence_ID,
    topo.Topography,
    s_prev.Topo_code,
    morph.Morphology,
    morph.Morpho_code,
    s_prev.Sample_analyzed,
    s_prev.Sample_mutated,
    country.Population,
    country.Country,
    s_prev.Comment,
    s_ref.Ref_ID,
    s_ref.Cross_Ref_ID,
    s_ref.Title,
    s_ref.Authors,
    s_ref.S_Ref_Year,
    s_ref.Journal,
    s_ref.Volume,
    s_ref.Start_page,
    s_ref.End_page,
    s_ref.PubMed,
    s_ref.Comment AS Ref_comment,
    s_ref.Tissue_processing,
    s_ref.Start_material,
    s_ref.Prescreening,
    s_ref.Material_sequenced,
    s_ref.exon2,
    s_ref.exon3,
    s_ref.exon4,
    s_ref.exon5,
    s_ref.exon6,
    s_ref.exon7,
    s_ref.exon8,
    s_ref.exon9,
    s_ref.exon10,
    s_ref.exon11,
    s_ref.Exclude_analysis,
    s_ref.WGS_WXS
    FROM
    `{partial_id}.S_PREVALENCE` AS s_prev
    INNER JOIN
    `{partial_id}.S_REFERENCE` AS s_ref
    ON
    s_prev.Ref_ID = s_ref.Ref_ID
    INNER JOIN
    `{partial_id}.Topography_dic` AS topo
    ON
    s_prev.Topo_code = topo.Topo_code
    INNER JOIN
    `{partial_id}.Morphology_dic` AS morph
    ON
    s_prev.Morpho_ID = morph.Morphology_ID
    INNER JOIN
    `{partial_id}.Country_dic` AS country
    ON
    s_prev.Country_ID = country.Country_ID
    ORDER BY
    s_ref.Ref_ID
    
    '''
    
    return view_id, view_query 
    
def mouseModelView(partial_id):
    view_id = f'{partial_id}.MouseModelView'    
    view_query = f'''
    SELECT
    mm.MM_ID,
    mm.AAchange,
    aa_ch.AAchange_ID,
    mm.caMOD_ID,
    mm.ModelDescription,
    mm.TumorSites,
    mm.PubMed,
    mm.Mref_ID,
    aa_ch.ProtDescription,
    mm.Comment
    FROM
    `{partial_id}.AA_change` aa_ch
    RIGHT OUTER JOIN
    `{partial_id}.MOUSE_MODEL` mm
    ON
    aa_ch.AAchange = mm.AAchange
    ORDER BY
    mm.PubMed
    '''
    
    return view_id, view_query 

def inducedMutationView(partial_id):
    view_id = f'{partial_id}.InducedMutationView'    
    view_query = f'''
    SELECT 
    m.MUT_ID, l.hg19_Chr17_coordinates, l.hg38_Chr17_coordinates, l.Context_coding_3, m.g_description,
    m.g_description_GRCh38, m.c_description, aa_ch.ProtDescription, m.COSMIClink, e.Exposure, im.Model,
    im.Clone_ID, im.Add_Info, ir.IRef_ID, ir.PubMed, im.Induced_ID, l.Genomic_nt
    FROM            
    `{partial_id}.INDUCED_MUTATIONS` im INNER JOIN
    `{partial_id}.I_REFERENCE` ir ON im.IRef_ID = ir.IRef_ID INNER JOIN
    `{partial_id}.MUTATION` m ON im.MUT_ID = m.MUT_ID INNER JOIN
    `{partial_id}.AA_change` aa_ch ON m.AAchangeID = aa_ch.AAchange_ID INNER JOIN
    `{partial_id}.Exposure_dic` e ON im.Exposure_ID = e.Exposure_ID INNER JOIN
    `{partial_id}.Location` l ON m.Location_ID = l.Location_ID
    ORDER BY 
    e.Exposure
    '''
    
    return view_id, view_query 
    
def germlineTumorStats(partial_id):
    view_id = f'{partial_id}.GermlineTumorStats'    
    view_query = f'''
    SELECT
    topo.StatisticGraphGermline,
    COUNT(topo.StatisticGraphGermline) AS Count
    FROM
    `{partial_id}.G_INDIVIDUAL` AS ind
    INNER JOIN
    `{partial_id}.G_FAMILY` AS fam
    ON
    ind.Family_ID = fam.Family_ID
    INNER JOIN
    `{partial_id}.G_TUMOR` AS tum
    ON
    ind.Individual_ID = tum.Individual_ID
    INNER JOIN
    `{partial_id}.G_P53_MUTATION` AS mut
    ON
    fam.Family_ID = mut.Family_ID
    INNER JOIN
    `{partial_id}.Subtopography_dic` AS sub
    ON
    tum.Subtopo_ID = sub.Subtopo_ID
    INNER JOIN
    `{partial_id}.Topography_dic` AS topo
    ON
    sub.Topo_code = topo.Topo_code
    WHERE
    (LOWER(ind.Germline_carrier) = 'confirmed'
        OR LOWER(ind.Germline_carrier) = 'obligatory')
    AND (topo.Short_topo IS NOT NULL)
    AND (fam.Germline_mutation LIKE 'TP53%')
    GROUP BY
    topo.StatisticGraphGermline
    ORDER BY
    Count DESC

    
    '''
    
    return view_id, view_query 
    
def germlinePrevalenceView(partial_id):
    view_id = f'{partial_id}.GermlinePrevalenceView'    
    view_query = f'''
    SELECT
    p.Diagnosis,
    p.Cohort,
    p.Cases_Analyzed,
    p.Cases_mutated,
    p.Mutation_prevalence,
    p.Remark,
    r.PubMed,
    r.G_Ref_Year,
    p.G_Prevalence_ID
    FROM
    `{partial_id}.G_PREVALENCE` AS p
    INNER JOIN
    `{partial_id}.G_REFERENCE` AS r
    ON
    p.REF_ID =r.Ref_ID
    ORDER BY
    p.Diagnosis
    
    '''
    
    return view_id, view_query 


def germlineRefDownload(partial_id):
    view_id = f'{partial_id}.GermlineRefDownload'    
    view_query = f'''
        SELECT
        ref.Ref_ID,
        ref.Title,
        ref.Authors,
        ref.G_Ref_Year AS Year,
        ref.Journal,
        ref.Volume,
        ref.Start_page,
        ref.End_page,
        ref.PubMed,
        ref.Comment,
        fam.Other_infos
        FROM
        `{partial_id}.G_REFERENCE` AS ref
        INNER JOIN
        `{partial_id}.G_FAMILY` AS fam
        ON
        ref.Ref_ID = fam.Ref_ID
        WHERE
        (fam.Germline_mutation LIKE 'TP53%')
        GROUP BY ref.Ref_ID,
        ref.Title,
        ref.Authors,
        Year,
        ref.Journal,
        ref.Volume,
        ref.Start_page,
        ref.End_page,
        ref.PubMed,
        ref.Comment,
        fam.Other_infos
        ORDER BY
        ref.Ref_ID
    '''
    
    return view_id, view_query 

def germlineMutationStats(partial_id):
    view_id = f'{partial_id}.GermlineMutationStats'    
    view_query = f'''
    SELECT
    l.Codon_number,
    gc.Amino_Acid,
    seq.WT_AA,
    eff.Effect,
    t.Type,
    l.ExonIntron,
    m.Description,
    g_mut.Family_ID,
    m.Type_ID,
    m.Effect_ID,
    m.Mutant_nucleotide,
    m.Mutant_codon,
    aa.AAchange,
    m.Polymorphism,
    m.c_description
    FROM
    `{partial_id}.G_P53_MUTATION` AS g_mut
    INNER JOIN
    `{partial_id}.MUTATION` AS m
    ON
    g_mut.MUT_ID = m.MUT_ID
    INNER JOIN
    `{partial_id}.Location` AS l
    ON
    m.Location_ID = l.Location_ID
    INNER JOIN
    `{partial_id}.Genetic_code` AS gc
    ON
    m.Mutant_codon = gc.Codon
    INNER JOIN
    `{partial_id}.p53_sequence` AS seq
    ON
    l.Codon_number = seq.Codon_number
    INNER JOIN
    `{partial_id}.Effect_dic` AS eff
    ON
    m.Effect_ID = eff.Effect_ID
    INNER JOIN
    `{partial_id}.Type_dic` AS t
    ON
    m.Type_ID = t.Type_ID
    INNER JOIN
    `{partial_id}.AA_change` AS aa
    ON
    m.AAchangeID = aa.AAchange_ID
    WHERE
    lower(m.Polymorphism) != 'validated'
    '''
    
    return view_id, view_query     

def germlineDownload(partial_id):
    view_id = f'{partial_id}.GermlineDownload'    
    view_query = f'''
    SELECT
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
    CONCAT('https://www.ncbi.nlm.nih.gov/pubmed/',ref.PubMed) AS PubMedLink
    FROM
    `{partial_id}.G_FamilyCase_dic` AS fc
    RIGHT OUTER JOIN
    `{partial_id}.G_INDIVIDUAL` AS gi
    ON
    fc.FamilyCase_ID = gi.FamilyCase_ID
    RIGHT OUTER JOIN
    `{partial_id}.Effect_dic` AS eff
    INNER JOIN
    `{partial_id}.G_Classification_dic` AS cl
    INNER JOIN
    `{partial_id}.G_FAMILY` AS fam
    ON
    cl.Class_ID = fam.Class_ID
    INNER JOIN
    `{partial_id}.G_P53_MUTATION` AS mut
    ON
    fam.Family_ID = mut.Family_ID
    INNER JOIN
    `{partial_id}.G_REFERENCE` AS ref
    ON
    fam.Ref_ID = ref.Ref_ID
    INNER JOIN
    `{partial_id}.Country_dic` AS c
    ON
    fam.Country_ID = c.Country_ID
    INNER JOIN
    `{partial_id}.MUTATION` AS m
    ON
    mut.MUT_ID = m.MUT_ID
    INNER JOIN
    `{partial_id}.Genetic_code` AS code
    ON
    m.Mutant_codon = code.Codon
    ON
    eff.Effect_ID = m.Effect_ID
    INNER JOIN
    `{partial_id}.AA_change` AS aac
    ON
    m.AAchangeID = aac.AAchange_ID
    INNER JOIN
    `{partial_id}.Location` AS loc
    ON
    m.Location_ID = loc.Location_ID
    INNER JOIN
    `{partial_id}.p53_sequence` AS seq
    ON
    loc.Codon_number = seq.Codon_number
    INNER JOIN
    `{partial_id}.AA_codes` AS aa
    ON
    seq.WT_AA = aa.Three_letter_code
    INNER JOIN
    `{partial_id}.Type_dic` AS ty
    ON
    m.Type_ID = ty.Type_ID
    ON
    gi.Family_ID = fam.Family_ID
    LEFT OUTER JOIN
    `{partial_id}.Subtopography_dic` AS sub
    LEFT OUTER JOIN
    `{partial_id}.Topography_dic` AS td
    ON
    sub.Topo_code = td.Topo_code
    RIGHT OUTER JOIN
    `{partial_id}.Morphology_dic` AS md
    RIGHT OUTER JOIN
    `{partial_id}.G_TUMOR` AS gt
    ON
    md.Morphology_ID = gt.Morpho_ID
    ON
    sub.Subtopo_ID = gt.Subtopo_ID
    ON
    gi.Individual_ID = gt.Individual_ID
    WHERE
    (fam.Germline_mutation LIKE 'TP53%')
    ORDER BY
    fam.Ref_ID,
    fam.Family_ID
    '''
    
    return view_id, view_query 


def functionView(partial_id):
    view_id = f'{partial_id}.FunctionView'    
    view_query = f'''
        SELECT
        fp.Function_ID,
        aa_ch.AAchange,
        aa_ch.ProtDescription,
        seq.WT_AA,
        aa_ch.Codon_Number,
        aa_co.Three_letter_code AS Mutant_AA,
        aa_ch.SwissProtLink,
        seq.Structural_motif,
        fp.Codon72AA,
        fp.Conserved_WT_Function,
        fp.Loss_of_Function,
        fp.Dominant_Negative_Activity,
        fp.Gain_of_Function,
        fp.Temperature_Sensitivity,
        fp.Cell_assay,
        fp.Cell_lines,
        fr.FRef_ID,
        fr.Authors,
        fr.Title,
        fr.Ref_Year,
        fr.Journal,
        fr.Volume,
        fr.Start_page,
        fr.PubMed,
        aa_ch.AAchange_ID,
        aa_ch.Complex,
        fp.Temp_ref,
        fp.Assay_design,
        fp.Method
    FROM
    `{partial_id}.FUNCTION_PUB` fp
    INNER JOIN
    `{partial_id}.FUNCTION_REFERENCE` fr
    ON
    fp.FRef_ID = fr.FRef_ID
    INNER JOIN
    `{partial_id}.AA_change` aa_ch
    ON
    fp.AAchangeID = aa_ch.AAchange_ID
    INNER JOIN
    `{partial_id}.p53_sequence` seq
    ON
    aa_ch.Codon_Number = seq.Codon_number
    INNER JOIN
    `{partial_id}.AA_codes` aa_co
    ON
    aa_ch.Mutant_AA = aa_co.One_letter_code
    ORDER BY
    aa_ch.Codon_Number,
    Mutant_AA,
    fr.FRef_ID
    '''
    
    return view_id, view_query 

def functionIshiokaDownload(partial_id):
    view_id = f'{partial_id}.FunctionIshiokaDownload'
    view_query = f'''
    SELECT
    aa.ProtDescription,
    aa.AAchange,
    aa.Codon_Number,
    fi.WAF1nWT,
    fi.MDM2nWT,
    fi.BAXnWT,
    fi.h1433snWT,
    fi.AIP1nWT,
    fi.GADD45nWT,
    fi.NOXAnWT,
    fi.P53R2nWT,
    fi.WAF1nWT_Saos2,
    fi.MDM2nWT_Saos2,
    fi.BAXnWT_Saos2,
    fi.h1433snWT_Saos2,
    fi.AIP1nWT_Saos2,
    fi.PUMAnWT_Saos2,
    fi.SubG1nWT_Saos2,
    fi.Oligomerisation_yeast
    FROM
    `{partial_id}.FUNCTION_ISHIOKA` AS fi
    INNER JOIN
    `{partial_id}.AA_change` AS aa
    ON
    fi.AAchangeID = aa.AAchange_ID
    ORDER BY
    aa.Codon_Number
    '''
    return view_id, view_query
    
def functionDownload(partial_id):
    
    view_id = f'{partial_id}.FunctionDownload'
    view_query = f'''
    SELECT
        fp.Function_ID,
        aac.ProtDescription,
        aac.AAchange,
        aac.Codon_Number,
        seq.WT_AA,
        aa.Three_letter_code AS Mutant_AA,
        aac.SwissProtLink,
        seq.Structural_motif,
        fp.Codon72AA,
        fp.Conserved_WT_Function,
        fp.Loss_of_Function,
        fp.Dominant_Negative_Activity,
        fp.Gain_of_Function,
        fp.Temperature_Sensitivity,
        fp.Temp_ref,
        fp.Cell_assay,
        fp.Cell_lines,
        fp.Assay_design,
        fp.Method,
        fr.FRef_ID,
        fr.Authors,
        fr.Title,
        fr.Ref_Year AS Year,
        fr.Journal,
        fr.Volume,
        fr.Start_page,
        fr.PubMed
    FROM
        `{partial_id}.FUNCTION_REFERENCE` AS fr
    INNER JOIN
        `{partial_id}.AA_change` AS aac
    INNER JOIN
        `{partial_id}.FUNCTION_PUB` AS fp
    ON
        aac.AAchange_ID = fp.AAchangeID
    ON
        fr.FRef_ID = fp.FRef_ID
    INNER JOIN
        `{partial_id}.p53_sequence` AS seq
    ON
        aac.Codon_Number = seq.Codon_number
    INNER JOIN
        `{partial_id}.AA_codes` AS aa
    ON
        aac.Mutant_AA = aa.One_letter_code
    ORDER BY
        aac.Codon_Number,
        Mutant_AA
    
    '''
    return view_id, view_query

def cellLineSiteStats(partial_id):
    
    view_id = f'{partial_id}.CellLineSiteStats'
    view_query = f'''
    SELECT
        topo.StatisticGraph,
        topo.Short_topo,
        COUNT(topo.Short_topo) AS DatasetRx,
        samp.Sample_source_ID
    FROM
    `{partial_id}.S_SAMPLE` AS samp
    INNER JOIN
    `{partial_id}.Subtopography_dic` AS sub
    ON
    samp.Subtopo_ID = sub.Subtopo_ID
    INNER JOIN
    `{partial_id}.Topography_dic` AS topo
    ON
    sub.Topo_code = topo.Topo_code
    GROUP BY
    topo.StatisticGraph,
    topo.Short_topo,
    samp.Sample_source_ID
    HAVING
    (samp.Sample_source_ID = 4)
    ORDER BY
    COUNT(topo.Short_topo) DESC
    
    '''
    
    return view_id, view_query    

def cellLineMutationStats(partial_id):
    
    view_id = f'{partial_id}.CellLineMutationStats'
    view_query = f'''
    SELECT 
        s_mut.Mutation_ID, 
        l.Codon_number, 
        l.hg38_Chr17_coordinates, 
        l.hg19_Chr17_coordinates, 
        e.Effect,
        t.Type,
        m.Description, 
        m.Type_ID, 
        m.Effect_ID, 
        m.Mutant_nucleotide, 
        m.Mutant_codon, 
        s_sam.Sample_source_ID
    FROM 
        `{partial_id}.MUTATION` as m INNER JOIN
        `{partial_id}.Location` as l ON m.Location_ID = l.Location_ID INNER JOIN
        `{partial_id}.Genetic_code` as g ON m.Mutant_codon = g.Codon INNER JOIN
        `{partial_id}.p53_sequence` as s ON l.Codon_number = s.Codon_number INNER JOIN
        `{partial_id}.Effect_dic` as e ON m.Effect_ID = e.Effect_ID INNER JOIN
        `{partial_id}.Type_dic` as t ON m.Type_ID = t.Type_ID INNER JOIN
        `{partial_id}.S_MUTATION` as s_mut ON m.MUT_ID = s_mut.MUT_ID INNER JOIN
        `{partial_id}.S_SAMPLE` as s_sam ON s_mut.Sample_ID = s_sam.Sample_ID
    WHERE 
        s_sam.Sample_source_ID = 4
    
    '''
    
    return view_id, view_query
    

def cellLineDownload(partial_id):
    
    view_id = f'{partial_id}.CellLineDownload'
    view_query = f'''
    SELECT
        ss.Sample_ID,
        ss.Sample_Name,
        cl.ATCC_ID,
        cl.Cosmic_ID,
        td.Short_topo,
        md.Morphology,
        tod.Tumor_origin,
        cl.Add_info,
        si.Sex,
        ss.Age,
        cd.Country,
        cd.Population,
        si.Germline_mutation,
        si.Infectious_agent,
        tob.Tobacco,
        al.Alcohol,
        exd.Exposure,
        ss.KRAS_status,
        ss.Other_mutations,
        cl.TP53status,
        ss.p53_IHC,
        ss.LOH AS TP53_LOH,
        mu.MUT_ID,
        loc.hg18_Chr17_coordinates,
        loc.hg19_Chr17_coordinates,
        loc.hg38_Chr17_coordinates,
        loc.ExonIntron,
        loc.Codon_number,
        mu.Description,
        ty.Type,
        mu.c_description,
        mu.g_description,
        mu.g_description_GRCh38,
        loc.Base AS WT_nucleotide,
        mu.Mutant_nucleotide,
        seq.WT_codon,
        mu.Mutant_codon,
        seq.WT_AA,
        gc.Amino_Acid AS Mutant_AA,
        eff.Effect,
        aac.ProtDescription,
        mu.COSMIClink,
        mu.TCGA_ICGC_GENIE_count,
        seq.Hotspot,
        aac.AGVGDClass,
        aac.SIFTClass,
        aac.Polyphen2,
        aac.REVEL,
        aac.BayesDel,
        aac.StructureFunctionClass,
        aac.TransactivationClass,
        aac.DNEclass,
        aac.DNE_LOFclass,
        fi.WAF1nWT,
        fi.MDM2nWT,
        fi.BAXnWT,
        fi.AIP1nWT,
        fi.h1433snWT,
        fi.GADD45nWT,
        fi.NOXAnWT,
        fi.P53R2nWT,
        sr.Ref_ID,
        sr.Journal,
        sr.S_Ref_Year AS Year,
        sr.Volume,
        sr.Start_page,
        sr.PubMed,
        sr.Start_material,
        sr.Prescreening,
        sr.Material_sequenced,
        sr.exon2,
        sr.exon3,
        sr.exon4,
        sr.exon5,
        sr.exon6,
        sr.exon7,
        sr.exon8,
        sr.exon9,
        sr.exon10,
        sr.exon11
        FROM
        `{partial_id}.Location` AS loc
        LEFT OUTER JOIN
        `{partial_id}.p53_sequence` AS seq
        ON
        loc.Codon_number = seq.Codon_number
        RIGHT OUTER JOIN
        `{partial_id}.Effect_dic` AS eff
        RIGHT OUTER JOIN
        `{partial_id}.Type_dic` AS ty
        INNER JOIN
        `{partial_id}.MUTATION` AS mu
        ON
        ty.Type_ID = mu.Type_ID
        ON
        eff.Effect_ID = mu.Effect_ID
        LEFT OUTER JOIN
        `{partial_id}.AA_change` AS aac
        ON
        mu.AAchangeID = aac.AAchange_ID
        LEFT OUTER JOIN
        `{partial_id}.Genetic_code` AS gc
        ON
        mu.Mutant_codon = gc.Codon
        ON
        loc.Location_ID = mu.Location_ID
        RIGHT OUTER JOIN
        `{partial_id}.S_MUTATION` sm
        ON
        mu.MUT_ID = sm.MUT_ID
        RIGHT OUTER JOIN
        `{partial_id}.Tobacco_dic` AS tob
        INNER JOIN
        `{partial_id}.Alcohol_dic` AS al
        INNER JOIN
        `{partial_id}.CellLines` AS cl
        INNER JOIN
        `{partial_id}.S_REFERENCE` AS sr
        INNER JOIN
        `{partial_id}.S_INDIVIDUAL` AS si
        ON
        sr.Ref_ID = si.Ref_ID
        INNER JOIN
        `{partial_id}.S_SAMPLE` AS ss
        ON
        si.Individual_ID = ss.Individual_ID
        INNER JOIN
        `{partial_id}.Morphology_dic` AS md
        ON
        ss.Morpho_ID = md.Morphology_ID
        INNER JOIN
        `{partial_id}.Subtopography_dic` AS sub
        INNER JOIN
        `{partial_id}.Topography_dic` AS td
        ON
        sub.Topo_code = td.Topo_code
        ON
        ss.Subtopo_ID = sub.Subtopo_ID
        ON
        cl.Sample_ID = ss.Sample_ID
        INNER JOIN
        `{partial_id}.Country_dic` AS cd
        ON
        si.Country_ID = cd.Country_ID
        ON
        al.Alcohol_ID = si.Alcohol_ID
        ON
        tob.Tobacco_ID = si.Tobacco_ID
        INNER JOIN
        `{partial_id}.Exposure_dic` AS exd
        ON
        si.Exposure_ID = exd.Exposure_ID
        INNER JOIN
        `{partial_id}.Tumor_origin_dic` AS tod
        ON
        ss.Tumor_origin_ID = tod.Tumor_origin_ID
        ON
        sm.Sample_ID = ss.Sample_ID
        LEFT OUTER JOIN
        `{partial_id}.FUNCTION_ISHIOKA` AS fi
        ON
        aac.AAchange_ID = fi.AAchangeID
        WHERE
        (ss.Sample_source_ID = 4)
    '''
    return view_id, view_query
    



def splicingPredictionView(partial_id):
    view_id = f'{partial_id}.SPLICING_PREDICTION_VIEW'
    view_query = f'''
        SELECT * 
        FROM `{partial_id}.SPLICING_PREDICTION`
        WHERE Source NOT LIKE 'HSF%'
    '''
    return view_id, view_query


def main():
    '''
       1. Loops through the list and calls the function for the designated table 
       
       2. Arguments passed are the partial_id 
       
       3. generate_table takes two arguments view_id and view_query. View_id will contain the 
       full id ("Project.Dataset.Table"). view_query is the query which will create the view. 
       
    '''
    project = os.environ.get('GCLOUD_PROJECT', 'isb-cgc-tp53-test')
    dataset = 'P53_data'
    
    # Project ID and Dataset ID 
    parital_id = f'{project}.{dataset}'
    
    # Each function will upload a view 
    functions = [
        cellLineDownload,
        cellLineSiteStats,
        functionDownload,
        functionIshiokaDownload,
        functionView,
        germlineDownload,
        germlineMutationStats,
        germlinePrevalenceView,
        germlineRefDownload,
        germlineTumorStats,
        inducedMutationView,
        mouseModelView,
        prevalenceDownload,
        prevalenceStat,
        prevalenceView,
        prognosisDownload,
        somaticDownload,
        somaticRefDownload,
        somaticTumorStats,
        cellLineMutationStats,
        splicingPredictionView

    ]
    
    
    for function in functions:
        view_id, view_query = function(parital_id)
        generate_table(view_id,
                       view_query)

if __name__ == '__main__':
    main()