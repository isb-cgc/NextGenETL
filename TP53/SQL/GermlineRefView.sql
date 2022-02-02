-- GermlineRefView.sql
-- SQL query to mirror GermlineRefView
SELECT
  DISTINCT R.Ref_ID,
  R.Title,
  R.Authors,
  R.G_Ref_Year AS Year,
  R.Journal,
  R.Volume,
  R.Start_page,
  R.End_page,
  R.PubMed,
  R.Comment
FROM
  `isb-cgc-tp53-dev.P53_data.G_REFERENCE` AS R
INNER JOIN
  `isb-cgc-tp53-dev.P53_data.G_FAMILY` AS F
ON
  R.Ref_ID = F.Ref_ID
WHERE
  (LOWER(F.Germline_mutation) LIKE 'tp53%')
ORDER BY
  R.Ref_ID